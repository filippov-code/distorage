package healthcheck

import (
	"context"
	grpc_tools "distorage/internal/grpc/tools"
	"time"

	"sync"

	"github.com/google/uuid"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type checkProcess struct {
	friendId  uuid.UUID
	cancel    context.CancelFunc
	onFailure func(uuid.UUID)
}

type HealthcheckService struct {
	connPool         *grpc_tools.Pool
	defaultOnFailure func(uuid.UUID)
	checkProcesses   map[uuid.UUID]*checkProcess
}

func NewHealthcheckService(
	connPool *grpc_tools.Pool,
	defaultOnFailure func(uuid.UUID)) *HealthcheckService {
	return &HealthcheckService{
		connPool:         connPool,
		checkProcesses:   make(map[uuid.UUID]*checkProcess),
		defaultOnFailure: defaultOnFailure,
	}
}

func (s *HealthcheckService) Start(friendId uuid.UUID, url string, onFailure func(uuid.UUID)) error {

	var errorChan = make(chan error, 1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	if onFailure == nil {
		onFailure = s.defaultOnFailure
	}
	go s.start(friendId, url, onFailure, errorChan, &waitGroup)

	waitGroup.Wait()
	close(errorChan)

	for err := range errorChan {
		return err
	}

	return nil
}

func (s *HealthcheckService) start(friendId uuid.UUID, url string, onFailure func(uuid.UUID), errorChan chan error, wg *sync.WaitGroup) {
	var conn, err = s.connPool.For(url)
	if err != nil {
		errorChan <- err
		wg.Done()
		return
	}

	healthClient := healthpb.NewHealthClient(conn)

	var ctx, cancel = context.WithCancel(context.Background())

	s.checkProcesses[friendId] = &checkProcess{
		friendId:  friendId,
		cancel:    cancel,
		onFailure: onFailure,
	}

	wg.Done()

	var failure = func() {
		cancel()
		onFailure(friendId)
	}

	defer func() {
		if r := recover(); r != nil {
			failure()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			delete(s.checkProcesses, friendId)
			return
		default:

			res, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
			if res.Status.String() != "SERVING" || err != nil {
				failure()
			}

			time.Sleep(2 * time.Second)
		}
	}
}
