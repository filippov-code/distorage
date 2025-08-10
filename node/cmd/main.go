package main

import (
	"distorage/internal/boot"
	"distorage/internal/grpc/gateway"
	hc_server "distorage/internal/grpc/healthcheck"
	"distorage/internal/grpc/node"
	grpc_tools "distorage/internal/grpc/tools"
	hc "distorage/internal/healthcheck"

	"distorage/internal/recovery"
	rep "distorage/internal/repositories"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	health "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
)

var (
	meRepository           rep.MeRepositorier
	friendsRepository      rep.FriendsRepositorier
	metadataRepository     rep.MetadataRepositorier
	availabilityRepository rep.AvailabilitiesRepositorier
	storageRepository      rep.Storager
	healthcheckService     *hc.HealthcheckService
	connPool               *grpc_tools.Pool
)

type CLIParameters struct {
	Port            *string
	Url             *string
	BootUrl         *string
	MongoConnString *string
	MinioUrl        *string
	MinioLogin      *string
	MinioPassword   *string
}

func main() {
	params, err := parseCLIParameters()
	if err != nil {
		err := fmt.Errorf("ошибка парсинга параметров %v", err)
		panic(err)
	}

	initVars(params)

	var wg sync.WaitGroup
	wg.Add(1)
	go startGRPCServer(params, &wg)
	wg.Wait()

	if params.BootUrl != nil {

		var bootService = boot.NewBootService(meRepository, friendsRepository, metadataRepository, availabilityRepository, healthcheckService, connPool)
		err := bootService.Boot(*params.BootUrl)
		if err != nil {
			panic(fmt.Errorf("ошибка подключения к сети %v", err))
		}
	}

	select {}
}

func parseCLIParameters() (params *CLIParameters, error error) {
	port := flag.String("port", "", "Порт для запуска gRPC сервера")
	url := flag.String("url", "", "Адрес ноды")
	bootUrl := flag.String("boot", "", "Адрес boot-ноды для отправки приветствия (необязательный)")
	mongoConnString := flag.String("mongo", "", "Строка подключения к mongo")
	minioConnString := flag.String("minio", "", "Строка подключения к minio")

	flag.Parse()

	if *port == "" {
		return nil, errors.New("port параметр обязателен")
	}
	if *url == "" {
		return nil, errors.New("url параметр обязателен")
	}
	if *mongoConnString == "" {
		return nil, errors.New("mongo параметр обязателен")
	}
	if *bootUrl == "" {
		bootUrl = nil
	}

	minioConnectParts := strings.Split(*minioConnString, "@")
	minioCreds := strings.Split(strings.TrimPrefix(minioConnectParts[0], "minio://"), ":")
	if *minioConnString == "" || len(minioConnectParts) != 2 || len(minioCreds) != 2 {
		return nil, errors.New("minio параметр обязателен в формате minio://login:pass@url")
	}

	minioUrl := minioConnectParts[1]
	minioLogin := minioCreds[0]
	minioPassword := minioCreds[1]

	return &CLIParameters{
		Port:            port,
		Url:             url,
		BootUrl:         bootUrl,
		MongoConnString: mongoConnString,
		MinioUrl:        &minioUrl,
		MinioLogin:      &minioLogin,
		MinioPassword:   &minioPassword,
	}, nil
}

func initVars(params *CLIParameters) {
	var err error

	meRepository, err = rep.NewMongoMeRepository(*params.MongoConnString, *params.Url, *params.Port)
	if err != nil {
		panic(fmt.Errorf("ошибка инициализации: %v", err))
	}

	connPool = grpc_tools.NewPool()

	friendsRepository, err = rep.NewMongoFriendsRepository(*params.MongoConnString)
	if err != nil {
		panic(fmt.Errorf("ошибка инициализации: %v", err))
	}

	metadataRepository, err = rep.NewMongoMetadataRepository(*params.MongoConnString)
	if err != nil {
		panic(fmt.Errorf("ошибка инициализации: %v", err))
	}

	availabilityRepository, err = rep.NewMongoAvailabilitiesRepository(*params.MongoConnString)
	if err != nil {
		panic(fmt.Errorf("ошибка инициализации: %v", err))
	}

	storageRepository, err = rep.NewMinioStorage(*params.MinioUrl, *params.MinioLogin, *params.MinioPassword)
	if err != nil {
		panic(fmt.Errorf("ошибка инициализации: %v", err))
	}

	revoveryService := recovery.NewRecoveryService(*meRepository.Get(), friendsRepository, availabilityRepository, metadataRepository, storageRepository, connPool)
	healthcheckService = hc.NewHealthcheckService(connPool, revoveryService.Recovery)
}

func startGRPCServer(params *CLIParameters, wg *sync.WaitGroup) {
	listener, err := net.Listen("tcp", ":"+*params.Port)
	if err != nil {
		err := fmt.Errorf("ошибка при запуске прослушивания порта %v: %v", params.Port, err)
		panic(err)
	}

	grpcServer := grpc.NewServer()

	//Регистрация сервисов

	health.RegisterHealthServer(grpcServer, hc_server.NewServer())
	node.RegisterNodeServer(grpcServer, node.NewServer(meRepository, friendsRepository, metadataRepository, storageRepository, availabilityRepository, healthcheckService, connPool))
	gateway.RegisterGatewayServer(grpcServer, gateway.NewServer(meRepository, friendsRepository, metadataRepository, storageRepository, availabilityRepository, connPool))

	log.Printf("gRPC сервер запущен и слушает порт %s", meRepository.Get().Port)

	wg.Done()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Ошибка при запуске gRPC сервера: %v", err)
	}
}
