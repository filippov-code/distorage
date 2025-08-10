package boot

import (
	"context"
	"distorage/internal/grpc/node"
	grpc_tools "distorage/internal/grpc/tools"
	hc "distorage/internal/healthcheck"
	rep "distorage/internal/repositories"
	"distorage/internal/tools"
	"log"

	"github.com/google/uuid"
)

type BootService struct {
	meRepository           rep.MeRepositorier
	friendsRepository      rep.FriendsRepositorier
	metadataRepository     rep.MetadataRepositorier
	availabilityRepository rep.AvailabilitiesRepositorier
	healthcheckService     *hc.HealthcheckService
	connPool               *grpc_tools.Pool
}

func NewBootService(
	meRepository rep.MeRepositorier,
	friendsRepository rep.FriendsRepositorier,
	metadataRepository rep.MetadataRepositorier,
	availabilityRepository rep.AvailabilitiesRepositorier,
	healthcheckService *hc.HealthcheckService,
	connPool *grpc_tools.Pool,
) *BootService {
	return &BootService{
		meRepository:           meRepository,
		friendsRepository:      friendsRepository,
		metadataRepository:     metadataRepository,
		availabilityRepository: availabilityRepository,
		healthcheckService:     healthcheckService,
		connPool:               connPool,
	}
}

func (s *BootService) Boot(bootUrl string) error {

	log.Printf("Старт процесса подключения к сети")

	var me = s.meRepository.Get()
	var helloRequest = &node.HelloRequest{
		Id:   me.ID.String(),
		Name: me.ID.String(),
		Url:  me.URL,
	}

	//Отправляем приветствие знакомому
	var bootHelloResponse, err = s.sayHello(bootUrl, helloRequest)
	if err != nil {
		return err
	}

	//Отправляем приветствие остальным узлам сети
	var hellosResults = tools.Go(
		func(url string) (*node.HelloResponse, error) {
			return s.sayHello(url, helloRequest)
		},
		bootHelloResponse.FriendsUrls,
	)

	//проверка на ошибки
	for _, response := range hellosResults {
		if response.Error != nil {
			return err
		}
	}

	var helloResponses = tools.Select(
		hellosResults,
		func(r tools.GoResult[string, *node.HelloResponse]) *node.HelloResponse {
			return r.Result
		},
	)
	helloResponses = append(helloResponses, bootHelloResponse)

	//Подписываемся на узлы
	var subRequest = &node.SubscribeRequest{
		Id:   me.ID.String(),
		Name: me.ID.String(),
		Url:  me.URL,
	}

	var subResults = tools.Go(
		func(url string) (*node.SubscribeResponse, error) {
			return s.subscribe(url, subRequest)
		},
		append(bootHelloResponse.FriendsUrls, bootUrl),
	)

	for _, response := range subResults {
		if response.Error != nil {
			return err
		}
	}

	var subResponses = tools.Select(
		subResults,
		func(r tools.GoResult[string, *node.SubscribeResponse]) *node.SubscribeResponse {
			return r.Result
		},
	)

	for _, subResponse := range subResponses {
		var friendId = uuid.MustParse(subResponse.Id)

		s.friendsRepository.Insert(&rep.Friend{
			ID:   friendId,
			Name: subResponse.Name,
			URL:  subResponse.Url,
		})

		var startHCError = s.healthcheckService.Start(friendId, subResponse.Url, nil)
		if startHCError != nil {
			return startHCError
		}
	}

	//Сохраняем информацию о данных на узлах
	var metadatasHashMap = make(map[string]struct{})
	var metadatasValues = make([]*rep.Metadata, 0)
	var availabilitiesValues = make([]*rep.Availability, 0)
	for _, hello := range helloResponses {
		for _, metadata := range hello.AvailableMetadata {
			availabilitiesValues = append(availabilitiesValues, &rep.Availability{
				FileID:   uuid.MustParse(metadata.Id),
				FriendID: uuid.MustParse(hello.Id),
			})

			if _, exists := metadatasHashMap[metadata.Id]; exists {
				continue
			}

			metadatasHashMap[metadata.Id] = struct{}{}

			metadatasValues = append(metadatasValues, &rep.Metadata{
				ID:          uuid.MustParse(metadata.Id),
				Filename:    metadata.Filename,
				IsAvailable: false,
			})
		}
	}
	s.metadataRepository.Insert(metadatasValues...)
	s.availabilityRepository.Insert(availabilitiesValues...)

	log.Printf("Конец процесса подключения к сети")

	return nil
}

func (s *BootService) sayHello(url string, req *node.HelloRequest) (*node.HelloResponse, error) {
	var conn, err = s.connPool.For(url)
	if err != nil {
		return nil, err
	}

	var client = node.NewNodeClient(conn)

	return client.SayHello(context.Background(), req)
}

func (s *BootService) subscribe(url string, req *node.SubscribeRequest) (*node.SubscribeResponse, error) {
	var conn, err = s.connPool.For(url)
	if err != nil {
		return nil, err
	}

	var client = node.NewNodeClient(conn)

	return client.Subscribe(context.Background(), req)
}
