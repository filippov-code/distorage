package recovery

import (
	"context"
	"distorage/internal/config"
	"distorage/internal/grpc/node"
	grpc_tools "distorage/internal/grpc/tools"
	rep "distorage/internal/repositories"
	"distorage/internal/tools"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type RecoveryService struct {
	me                       rep.Me
	friendsRepository        rep.FriendsRepositorier
	availabilitiesRepository rep.AvailabilitiesRepositorier
	metadataRepository       rep.MetadataRepositorier
	storagerRepository       rep.Storager
	connPool                 *grpc_tools.Pool
}

func NewRecoveryService(
	me rep.Me,
	friendsRepository rep.FriendsRepositorier,
	availabilitiesRepository rep.AvailabilitiesRepositorier,
	metadataRepository rep.MetadataRepositorier,
	storageRepository rep.Storager,
	connPool *grpc_tools.Pool) *RecoveryService {
	return &RecoveryService{
		me:                       me,
		friendsRepository:        friendsRepository,
		availabilitiesRepository: availabilitiesRepository,
		metadataRepository:       metadataRepository,
		storagerRepository:       storageRepository,
		connPool:                 connPool,
	}
}

func (s *RecoveryService) Recovery(friendId uuid.UUID) {

	log.Printf("Старт процесса восстановления для узла id(%s)", friendId)

	var recipients = s.getRecipients(friendId)
	var replicasForMe = recipients[s.me.ID]

	s.deleteLostFriend(friendId)

	if len(replicasForMe) > 0 {
		s.loadReplicas(replicasForMe)
	}

	log.Printf("Конец процесса восстановления")
}

func (s *RecoveryService) loadReplicas(replicasIds []uuid.UUID) {
	var availabilities = s.availabilitiesRepository.GetByFileIDs(replicasIds...)

	var shuffled = tools.Shuffle(availabilities, nil)

	var friendIdByFileId = make(map[uuid.UUID]uuid.UUID)
	var friendIds = make([]uuid.UUID, 0)
	for _, v := range shuffled {
		if _, exists := friendIdByFileId[v.FileID]; exists {
			continue
		}

		friendIdByFileId[v.FileID] = v.FriendID
		friendIds = append(friendIds, v.FriendID)
	}

	var friends = s.friendsRepository.GetMany(friendIds)
	var friendUrlById = make(map[uuid.UUID]string)
	for _, friend := range friends {
		friendUrlById[friend.ID] = friend.URL
	}

	tools.Go(
		func(fileId uuid.UUID) (*struct{}, error) {
			var friendId = friendIdByFileId[fileId]
			var url = friendUrlById[friendId]

			err := s.loadReplicaFromFriend(fileId, url)

			s.availabilitiesRepository.Insert(&rep.Availability{
				FileID:   fileId,
				FriendID: s.me.ID,
			})

			s.metadataRepository.SetIsAvailable(fileId, true)

			return &struct{}{}, err
		},
		replicasIds,
	)

	//разослать всем информацию о наличии файла на этом узле
	var req = &node.SendRecoveryMetadataRequest{
		FromId:  s.me.ID.String(),
		FileIds: tools.Select(replicasIds, func(id uuid.UUID) string { return id.String() }),
	}

	tools.Go(
		func(friend *rep.Friend) (bool, error) {
			var conn, err = s.connPool.For(friend.URL)
			if err != nil {
				return false, err
			}
			var client = node.NewNodeClient(conn)

			if _, err := client.SendRecoveryMetadata(context.Background(), req); err != nil {
				return false, err
			}

			return true, nil
		},
		friends)
}

func (s *RecoveryService) loadReplicaFromFriend(replicaId uuid.UUID, friendUrl string) error {
	conn, err := s.connPool.For(friendUrl)
	if err != nil {
		return err
	}

	client := node.NewNodeClient(conn)
	stream, err := client.GetReplica(context.Background(), &node.GetReplicaRequest{
		Id: replicaId.String(),
	})
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer pw.Close()
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if _, err := pw.Write(resp.Chunk); err != nil {
				return err
			}
		}
	})

	g.Go(func() error {
		return s.storagerRepository.Save(replicaId, pr, -1)
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("Не удалось загрузить реплику %s %w", replicaId, err)
	}

	log.Printf("Реплика id(%s) сохранена", replicaId)
	return nil
}

func (s *RecoveryService) deleteLostFriend(friendId uuid.UUID) {
	s.availabilitiesRepository.DeleteByFriendID(friendId)
	s.friendsRepository.Delete(friendId)
}

// Возвращает map[friendId][]fileId
func (s *RecoveryService) getRecipients(lostFriendId uuid.UUID) map[uuid.UUID][]uuid.UUID {
	var availabilities = s.availabilitiesRepository.GetAll()

	var allFriends = s.friendsRepository.GetAll()
	var allFriendsIds = tools.Select(allFriends, func(f *rep.Friend) uuid.UUID { return f.ID })
	var actualFriendsIds = tools.Where(allFriendsIds, func(id uuid.UUID) bool { return id != lostFriendId })
	var actualNodes = append(actualFriendsIds, s.me.ID)

	var fileToFriends = make(map[uuid.UUID]map[uuid.UUID]struct{})
	var friendToFiles = make(map[uuid.UUID]map[uuid.UUID]struct{})
	for _, a := range availabilities {
		if a.FriendID == lostFriendId {
			continue
		}

		if fileToFriends[a.FileID] == nil {
			fileToFriends[a.FileID] = make(map[uuid.UUID]struct{})
		}

		if friendToFiles[a.FriendID] == nil {
			friendToFiles[a.FriendID] = make(map[uuid.UUID]struct{})
		}

		fileToFriends[a.FileID][a.FriendID] = struct{}{}
		friendToFiles[a.FriendID][a.FileID] = struct{}{}
	}

	var fileToRecepients = make(map[uuid.UUID][]uuid.UUID)
	for fileId, friendsIds := range fileToFriends {
		if len(friendsIds) >= config.RequiredCopies {
			continue
		}

		var candidates = tools.Except(actualNodes, tools.Keys(friendsIds))

		fileToRecepients[fileId] = getRecipientsFromCandidates(fileId, config.RequiredCopies, candidates)
	}

	var recepientToFiles = make(map[uuid.UUID][]uuid.UUID)
	for fileId, friendsIds := range fileToRecepients {
		for _, friendId := range friendsIds {
			recepientToFiles[friendId] = append(recepientToFiles[friendId], fileId)
		}
	}

	return recepientToFiles
}

func getRecipientsFromCandidates(lostFileId uuid.UUID, count int, candidatesIds []uuid.UUID) []uuid.UUID {
	var hashcode = getHashCodeByUUID(lostFileId)
	var random = rand.NewSource(int64(hashcode))
	tools.Shuffle(candidatesIds, random)

	return tools.SkipTake(candidatesIds, 0, count)
}

func getHashCodeByUUID(id uuid.UUID) uint32 {
	hasher := fnv.New32a()
	hasher.Write(id[:])
	return hasher.Sum32()
}
