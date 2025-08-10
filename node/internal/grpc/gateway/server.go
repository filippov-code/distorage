package gateway

import (
	"context"
	"distorage/internal/config"
	"distorage/internal/grpc/node"
	grpc_tools "distorage/internal/grpc/tools"
	rep "distorage/internal/repositories"
	"distorage/internal/tools"
	"fmt"
	"io"
	"log"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type Server struct {
	UnimplementedGatewayServer
	me                     *rep.Me
	friendsRepository      rep.FriendsRepositorier
	availabilityRepository rep.AvailabilitiesRepositorier
	metadataRepository     rep.MetadataRepositorier
	storageRepository      rep.Storager
	connPool               *grpc_tools.Pool
}

func NewServer(
	meRepository rep.MeRepositorier,
	friendsRepository rep.FriendsRepositorier,
	metadataRepository rep.MetadataRepositorier,
	storageRepository rep.Storager,
	availabilityRepository rep.AvailabilitiesRepositorier,
	connPool *grpc_tools.Pool) *Server {
	var me = meRepository.Get()
	return &Server{
		me:                     me,
		friendsRepository:      friendsRepository,
		availabilityRepository: availabilityRepository,
		metadataRepository:     metadataRepository,
		storageRepository:      storageRepository,
		connPool:               connPool,
	}
}

/*
Принимает входящий stream UploadRequest от клиента (файл)
-Генерирует UUID
-Делает FanOut потока на N получателей:
---Отправляет поток в другие ноды
---Отправляет поток в Storager.Save
-Работает потоково
*/
func (s *Server) Upload(stream Gateway_UploadServer) error {
	log.Printf("Получен файл")

	res, err := grpc_tools.GRPCStreamAsReader(
		stream,
		func() (*UploadRequest, []byte, error) {
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

	id := uuid.New()
	friends := s.friendsRepository.GetAll()

	tools.Shuffle(friends, nil)
	friendsForReplica := tools.SkipTake(friends, 0, config.RequiredCopies-1)
	friendsForMetadata := tools.SkipTake(friends, config.RequiredCopies-1, len(friends))
	friendsForReplicaIds := tools.Select(friendsForReplica, func(f *rep.Friend) string { return f.ID.String() })

	metadataReq := &node.SendUploadedMetadataRequest{
		Id:                 id.String(),
		Filename:           res.First.Filename,
		FriendsWithReplica: append(friendsForReplicaIds, s.me.ID.String()),
	}

	// fanout на узлы + storager
	readers, err := tools.FanOutStream(res.Reader, len(friendsForReplica)+1)
	if err != nil {
		return fmt.Errorf("ошибка разделения потока: %v", err)
	}

	//Сохранение файла

	s.metadataRepository.Insert(&rep.Metadata{
		ID:          id,
		Filename:    metadataReq.Filename,
		IsAvailable: true,
	})

	var storageReader = readers[len(readers)-1]

	go s.storageRepository.Save(id, storageReader, -1)

	// отправка реплик узлам

	friendUrlsWithReplicaStream := make([]*friendUrlWithReplicaStream, len(friendsForReplica))
	for i, v := range friendsForReplica {
		friendUrlsWithReplicaStream[i] = &friendUrlWithReplicaStream{
			url:    v.URL,
			reader: readers[i],
		}
	}
	var sendReplicaResults = tools.Go(
		func(pair *friendUrlWithReplicaStream) (*struct{}, error) {
			var conn, err = s.connPool.For(pair.url)
			if err != nil {
				return nil, err
			}

			if _, err := sendReplicaStream(context.Background(), conn, metadataReq, pair.reader); err != nil {
				return nil, err
			}

			return &struct{}{}, nil
		},
		friendUrlsWithReplicaStream)

	var hasSendReplicasError = false

	for _, v := range sendReplicaResults {
		if v.Error != nil {
			hasSendReplicasError = true
			log.Printf("ошибка отправки реплики на узел с url: %v, error: %v", v.Param.url, v.Error)
		}
	}
	if hasSendReplicasError {
		return fmt.Errorf("ошибка при рассылке реплик")
	}

	// отправка метаданных всем узлам

	var sendMetadataResults = tools.Go(
		func(friend *rep.Friend) (bool, error) {
			var conn, err = s.connPool.For(friend.URL)
			if err != nil {
				return false, err
			}

			var client = node.NewNodeClient(conn)
			if _, err := client.SendUploadedMetadata(context.Background(), metadataReq); err != nil {
				return false, err
			}
			log.Printf("реплика отправлена на %v", friend.URL)

			return true, nil
		},
		friendsForMetadata)

	var hasSendMetadataError = false

	for _, v := range sendMetadataResults {
		if v.Error != nil {
			hasSendMetadataError = true
			log.Printf("ошибка отправки метаданных на узел %v, url: %v, error: %v", v.Param.ID, v.Param.URL, v.Error)
		}
	}
	if hasSendMetadataError {
		return fmt.Errorf("ошибка при рассылке метаданных")
	}

	// сохранение данных о наличии файлов на узлах

	var availabilities = tools.Select(
		friendsForReplica,
		func(friend *rep.Friend) *rep.Availability {
			return &rep.Availability{
				FileID:   id,
				FriendID: friend.ID,
			}
		},
	)
	availabilities = append(availabilities, &rep.Availability{
		FileID:   id,
		FriendID: s.me.ID,
	})
	s.availabilityRepository.Insert(availabilities...)

	response := &UploadResponse{
		Id: id.String(),
	}

	return stream.SendAndClose(response)
}

func sendReplicaStream(
	ctx context.Context,
	conn *grpc.ClientConn,
	metadata *node.SendUploadedMetadataRequest,
	r io.Reader,
) (*node.SendUploadedReplicaResponse, error) {
	client := node.NewNodeClient(conn)
	stream, err := client.SendUploadedReplica(ctx)
	if err != nil {
		return nil, err
	}

	// Отправка метаданных
	err = stream.Send(&node.SendUploadedReplicaRequest{
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
	}

	log.Printf("Метаданные реплики отправлены")

	// Отправка файла чанками
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			err := stream.Send(&node.SendUploadedReplicaRequest{
				Chunk: buf[:n],
			})
			if err != nil {
				return nil, err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("+return err = %v", err)
			return nil, err
		}
	}

	log.Printf("Закрытие стрима и получение ответа")
	// Закрытие стрима и получение ответа
	return stream.CloseAndRecv()
}

type friendUrlWithReplicaStream struct {
	url    string
	reader io.Reader
}

func (s *Server) Update(ctx context.Context, req *UpdateRequest) (*UpdateResponse, error) {
	log.Printf("Обновление")
	friends := s.friendsRepository.GetAll()

	for _, f := range friends {
		conn, _ := s.connPool.For(f.URL)
		client := node.NewNodeClient(conn)

		client.UpdateMetadata(ctx, &node.UpdateMetadataRequest{
			Id:       req.Id,
			Filename: req.Filename,
		})
	}

	s.metadataRepository.Update(uuid.MustParse(req.Id), req.Filename)

	return &UpdateResponse{}, nil
}

func (s *Server) Get(req *GetRequest, responseStream Gateway_GetServer) error {
	id := uuid.MustParse(req.Id)
	metadata, _ := s.metadataRepository.Get(id)

	if metadata.IsAvailable {
		reader, _ := s.storageRepository.Get(id)
		defer reader.Close()

		const chunk = 256 * 1024
		buf := make([]byte, chunk)

		for {
			n, rerr := reader.Read(buf)
			if n > 0 {
				// обязательно копируем: gRPC сериализует асинхронно
				if err := responseStream.Send(&GetResponse{
					Filename: metadata.Filename,
					Chunk:    append([]byte(nil), buf[:n]...),
				}); err != nil {
					return fmt.Errorf("ошибка отправки чанка: %w", err)
				}
			}

			switch rerr {
			case nil:
				// читаем дальше
			case io.EOF:
				return nil
			default:
				return fmt.Errorf("ошибка чтения: %w", rerr)
			}
		}

	} else {
		availabilities := s.availabilityRepository.GetByFileIDs(id)
		friendIds := tools.Select(availabilities, func(a *rep.Availability) uuid.UUID { return a.FriendID })
		friends := s.friendsRepository.GetMany(friendIds)
		friend := tools.SkipTake(friends, 0, 1)[0]
		conn, _ := s.connPool.For(friend.URL)
		client := node.NewNodeClient(conn)

		// Тянем файл у дружеского узла
		up, err := client.GetReplica(
			context.Background(),
			&node.GetReplicaRequest{Id: req.Id},
		)
		if err != nil {
			return status.Errorf(codes.Unavailable, "ошибка upstream: %v", err)
		}

		// Копируем сообщения
		for {
			resp, err := up.Recv()
			switch {
			case err == io.EOF:
				return nil
			case err != nil:
				return status.Errorf(codes.Unavailable, "ошибка upstream: %v", err)
			}

			if err := responseStream.Send(&GetResponse{
				Filename: metadata.Filename,
				Chunk:    resp.Chunk,
			}); err != nil {
				return status.Errorf(codes.Unavailable, "ошибка downstream: %v", err)
			}
		}
	}
}

func (s *Server) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "некорректный id: %v", err)
	}

	log.Printf("Удаление файла из сети %s", id)

	//Находим друзей, у которых есть реплики
	availabilities := s.availabilityRepository.GetByFileIDs(id)
	log.Printf("availabilities: len = %d", len(availabilities))
	friendIds := tools.Select(availabilities, func(a *rep.Availability) uuid.UUID { return a.FriendID })
	log.Printf("friendIds: len = %d", len(friendIds))
	friends := s.friendsRepository.GetMany(friendIds)
	log.Printf("friends: len = %d", len(friends))

	//Рассылаем delete на все узлы
	for _, f := range friends {
		conn, err := s.connPool.For(f.URL)
		if err != nil {
			log.Printf("ошибка соединения с узлом %s: %v", f.URL, err)
			continue
		}
		client := node.NewNodeClient(conn)
		_, err = client.DeleteReplica(ctx, &node.DeleteReplicaRequest{Id: req.Id})
		if err != nil {
			log.Printf("ошибка удаления на узле %s: %v", f.URL, err)
		}
	}

	log.Printf("Удаление файла %s", id)
	if err := s.metadataRepository.Delete(id); err != nil {
		return nil, status.Errorf(codes.Internal, "ошибка удаления метаданных: %v", err)
	}
	if err := s.availabilityRepository.DeleteByFileID(id); err != nil {
		log.Printf("ошибка удаления связей: %v", err)
	}
	if err := s.storageRepository.Delete(id); err != nil {
		log.Printf("ошибка удаления файла: %v", err)
	}
	log.Printf("Файл %s удалён", id)

	log.Printf("Отправка удаления в сеть %s", id)

	log.Printf("Файл %s успешно удалён из сети", id)
	return &DeleteResponse{}, nil
}
