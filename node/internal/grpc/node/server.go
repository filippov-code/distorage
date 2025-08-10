package node

import (
	"context"
	grpc_tools "distorage/internal/grpc/tools"
	hc "distorage/internal/healthcheck"
	rep "distorage/internal/repositories"
	"distorage/internal/tools"
	"fmt"
	"io"
	"log"

	"github.com/google/uuid"
)

type Server struct {
	UnimplementedNodeServer
	me                     *rep.Me
	friendsRepository      rep.FriendsRepositorier
	metadataRepository     rep.MetadataRepositorier
	storageRepository      rep.Storager
	availabilityRepository rep.AvailabilitiesRepositorier
	healthcheckService     *hc.HealthcheckService
	connPool               *grpc_tools.Pool
}

func NewServer(
	meRepository rep.MeRepositorier,
	friendsRepository rep.FriendsRepositorier,
	metadataRepository rep.MetadataRepositorier,
	storageRepository rep.Storager,
	availabilityRepository rep.AvailabilitiesRepositorier,
	healthCheckService *hc.HealthcheckService,
	connPool *grpc_tools.Pool) *Server {

	var me = meRepository.Get()
	return &Server{
		me:                     me,
		friendsRepository:      friendsRepository,
		metadataRepository:     metadataRepository,
		storageRepository:      storageRepository,
		availabilityRepository: availabilityRepository,
		healthcheckService:     healthCheckService,
		connPool:               connPool,
	}
}

func (s Server) SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {

	var friendsUrls = make([]string, 0)
	for _, friend := range s.friendsRepository.GetAll() {
		friendsUrls = append(friendsUrls, friend.URL)
	}

	var metadatas = make([]*HelloResponseMetadata, 0)
	for _, metadata := range s.metadataRepository.GetAvailable() {
		metadatas = append(metadatas, &HelloResponseMetadata{
			Id:       metadata.ID.String(),
			Filename: metadata.Filename,
		})
	}

	return &HelloResponse{
		Id:                s.me.ID.String(),
		Name:              s.me.ID.String(),
		Url:               s.me.URL,
		FriendsUrls:       friendsUrls,
		AvailableMetadata: metadatas,
	}, nil
}

func (s Server) Subscribe(ctx context.Context, req *SubscribeRequest) (*SubscribeResponse, error) {

	//Проверка ноды на доступность
	var conn, err = s.connPool.For(req.Url)
	if err != nil {
		return nil, err
	}

	log.Printf("Получен запрос на подписку от %s id(%s)", req.Url, req.Id)

	var hello = &HelloRequest{
		Id:   s.me.ID.String(),
		Name: s.me.ID.String(),
		Url:  s.me.URL,
	}
	var client = NewNodeClient(conn)
	if _, err := client.SayHello(context.Background(), hello); err != nil {
		return nil, err
	}

	err = s.friendsRepository.Insert(&rep.Friend{
		ID:   uuid.MustParse(req.Id),
		Name: req.Name,
		URL:  req.Url,
	})
	if err != nil {
		return nil, err
	}

	var startHCError = s.healthcheckService.Start(uuid.MustParse(req.Id), req.Url, nil)
	if startHCError != nil {
		return nil, startHCError
	}

	return &SubscribeResponse{
		Id:   s.me.ID.String(),
		Name: s.me.ID.String(),
		Url:  s.me.URL,
	}, nil
}

func (s Server) SendUploadedMetadata(ctx context.Context, req *SendUploadedMetadataRequest) (*SendUploadedMetadataResponse, error) {
	log.Printf("Получены метаданные реплики %s от id(%s)", req.Filename, req.Id)

	var fileId = uuid.MustParse(req.Id)

	s.metadataRepository.Insert(&rep.Metadata{
		ID:          fileId,
		Filename:    req.Filename,
		IsAvailable: false,
	})

	var availabilities = tools.Select(
		req.FriendsWithReplica,
		func(friend string) *rep.Availability {
			return &rep.Availability{
				FileID:   fileId,
				FriendID: uuid.MustParse(friend),
			}
		},
	)

	s.availabilityRepository.Insert(availabilities...)

	return &SendUploadedMetadataResponse{}, nil
}

func (s *Server) SendUploadedReplica(stream Node_SendUploadedReplicaServer) error {
	req, err := grpc_tools.GRPCStreamAsReader(
		stream,
		func() (*SendUploadedReplicaRequest, []byte, error) {
			req, err := stream.Recv()
			if err != nil {
				return nil, nil, err
			}
			return req, req.Chunk, nil
		},
	)
	if err != nil {
		return err
	}

	var fileId = uuid.MustParse(req.First.Metadata.Id)

	log.Printf("Получена реплика %s", fileId)

	s.storageRepository.Save(fileId, req.Reader, -1)

	s.metadataRepository.Insert(&rep.Metadata{
		ID:          fileId,
		Filename:    req.First.Metadata.Filename,
		IsAvailable: true,
	})

	var availabilities = tools.Select(
		req.First.Metadata.FriendsWithReplica,
		func(friend string) *rep.Availability {
			return &rep.Availability{
				FileID:   fileId,
				FriendID: uuid.MustParse(friend),
			}
		},
	)

	s.availabilityRepository.Insert(availabilities...)

	return stream.SendAndClose(&SendUploadedReplicaResponse{})
}

func (s Server) SendRecoveryMetadata(ctx context.Context, req *SendRecoveryMetadataRequest) (*SendRecoveryMetadataResponse, error) {
	log.Printf("Получены метаданные восстановления от %s", req.FromId)

	var availablities = tools.Select(
		req.FileIds,
		func(fileId string) *rep.Availability {
			return &rep.Availability{
				FileID:   uuid.MustParse(fileId),
				FriendID: uuid.MustParse(req.FromId),
			}
		},
	)

	s.availabilityRepository.Insert(availablities...)

	return &SendRecoveryMetadataResponse{}, nil
}

func (s Server) GetReplica(req *GetReplicaRequest, stream Node_GetReplicaServer) error {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return fmt.Errorf("некорректный uuid: %w", err)
	}

	reader, err := s.storageRepository.Get(id)
	if err != nil {
		return fmt.Errorf("файл не найден: %w", err)
	}
	defer reader.Close()

	const chunk = 256 * 1024
	buf := make([]byte, chunk)
	sent := int64(0)

	for {
		n, rerr := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&GetReplicaResponse{
				Chunk: append([]byte(nil), buf[:n]...),
			}); err != nil {
				return err
			}
			sent += int64(n)
		}

		switch rerr {
		case nil:
		case io.EOF:
			return nil
		default:
			return fmt.Errorf("ошибка чтения: %w", rerr)
		}
	}
}

func (s Server) Debug(ctx context.Context, req *DebugRequest) (*DebugResponse, error) {
	var friends = make([]*DebugResponseFriend, 0)
	for _, friend := range s.friendsRepository.GetAll() {
		friends = append(friends, &DebugResponseFriend{
			Id:  friend.ID.String(),
			Url: friend.URL,
		})
	}

	var metadatas = make([]*DebugResponseMetadata, 0)
	for _, metadata := range s.metadataRepository.GetAll() {
		metadatas = append(metadatas, &DebugResponseMetadata{
			Id:          metadata.ID.String(),
			Filename:    metadata.Filename,
			IsAvailable: metadata.IsAvailable,
		})
	}

	var availables = make([]*DebugResponseAvailable, 0)
	for _, a := range s.availabilityRepository.GetAll() {
		availables = append(availables, &DebugResponseAvailable{
			FileId:   a.FileID.String(),
			FriendId: a.FriendID.String(),
		})
	}

	return &DebugResponse{
		Id:                s.me.ID.String(),
		Name:              s.me.ID.String(),
		Url:               s.me.URL,
		Friends:           friends,
		AvailableMetadata: metadatas,
		Availables:        availables,
	}, nil
}

func (s Server) UpdateMetadata(ctx context.Context, req *UpdateMetadataRequest) (*UpdateMetadataResponse, error) {
	log.Printf("Получен запрос на обновление файла %s", req.Id)

	id := uuid.MustParse(req.Id)

	s.metadataRepository.Update(id, req.Filename)

	log.Printf("Файл %s обновлён", req.Id)

	return &UpdateMetadataResponse{}, nil
}

func (s Server) DeleteReplica(ctx context.Context, req *DeleteReplicaRequest) (*DeleteReplicaResponse, error) {
	log.Printf("Получен запрос на удаление файла %s", req.Id)

	id := uuid.MustParse(req.Id)
	if err := s.metadataRepository.Delete(id); err != nil {
		log.Printf("ошибка удаления метаданных: %v", err)
	}
	if err := s.availabilityRepository.DeleteByFileID(id); err != nil {
		log.Printf("ошибка удаления связей: %v", err)
	}
	if err := s.storageRepository.Delete(id); err != nil {
		log.Printf("ошибка удаления файла: %v", err)
	}

	log.Printf("Файл %s удалён", id)

	return &DeleteReplicaResponse{}, nil
}
