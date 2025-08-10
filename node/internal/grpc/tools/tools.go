package tools

import (
	"io"

	"google.golang.org/grpc"
)

type GRPCStreamAsReaderResult[Req any, Resp any] struct {
	Reader io.Reader
	First  Req
}

/*
Принимает gRPC stream
Читает stream.Recv()

Возвращает:
io.Reader — поток данных (собранный из chunk-ов)
Первый Request, если нужно (например, метаданные)
*/
func GRPCStreamAsReader[Req any, Res any](
	stream grpc.ClientStreamingServer[Req, Res],
	recv func() (*Req, []byte, error),
) (*GRPCStreamAsReaderResult[*Req, *Res], error) {

	//Получаем первое сообщение с метаданными
	firstReq, _, err := recv()
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	// Запускаем чтение в фоне
	go func() {
		defer pw.Close()

		for {
			_, chunk, err := recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}

			if _, err := pw.Write(chunk); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	// Возвращаем pipeReader и способ завершения
	return &GRPCStreamAsReaderResult[*Req, *Res]{
		Reader: pr,
		First:  firstReq,
	}, nil
}
