package tools

import (
	"io"
	"math/rand"
	"sync"
	"time"
)

// Тасование Фишера-Йетса
// randSource можеть быть nil
func Shuffle[T any](slice []T, randSource rand.Source) []T {
	if randSource == nil {
		randSource = rand.NewSource(time.Now().UnixNano())
	}

	var shuffled = make([]T, len(slice))
	copy(shuffled, slice)

	var rnd = rand.New(randSource)

	var n = len(shuffled)
	for i := n - 1; i > 0; i-- {
		j := rnd.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled
}

// map, аналог Select из C#
func Select[IN any, OUT any](slice []IN, selector func(IN) OUT) []OUT {
	var res = make([]OUT, len(slice))

	for i, v := range slice {
		res[i] = selector(v)
	}

	return res
}

func Where[T any](slice []T, predicate func(T) bool) []T {
	var res = make([]T, 0)

	for _, v := range slice {
		if predicate(v) {
			res = append(res, v)
		}
	}

	return res
}

func Group[T any, K comparable, V any](slice []T, keySelector func(T) K, valueSelector func(T) V) map[K][]V {
	lookup := make(map[K][]V)

	for _, item := range slice {
		key := keySelector(item)
		value := valueSelector(item)
		lookup[key] = append(lookup[key], value)
	}

	return lookup
}

// Первые count элементов
func SkipTake[T any](slice []T, skip int, take int) []T {
	if skip >= len(slice) {
		return []T{}
	}

	var end = skip + take
	if end > len(slice) {
		end = len(slice)
	}

	return slice[skip:end]
}

func Except[T comparable](a, b []T) []T {
	bSet := make(map[T]struct{}, len(b))
	for _, v := range b {
		bSet[v] = struct{}{}
	}

	var result []T
	for _, v := range a {
		if _, found := bSet[v]; !found {
			result = append(result, v)
		}
	}
	return result
}

func Keys[K comparable, V any](m map[K]V) []K {
	var res = make([]K, len(m))
	var i = 0
	for key := range m {
		res[i] = key
		i++
	}
	return res
}

type GoResult[P any, R any] struct {
	Param  P
	Result R
	Error  error
}

// Запуск функции в горутине для коллекции параметров
func Go[IN any, OUT any](f func(IN) (OUT, error), params []IN) []GoResult[IN, OUT] {
	var paramsLen = len(params)

	var wg sync.WaitGroup
	wg.Add(paramsLen)
	var resultsChan = make(chan GoResult[IN, OUT], paramsLen)

	for _, v := range params {
		go func(param IN, wg *sync.WaitGroup, resultsChan chan GoResult[IN, OUT]) {
			var res, err = f(v)

			resultsChan <- GoResult[IN, OUT]{
				Param:  param,
				Result: res,
				Error:  err,
			}

			wg.Done()

		}(v, &wg, resultsChan)
	}

	wg.Wait()
	close(resultsChan)

	var results = make([]GoResult[IN, OUT], paramsLen)
	var i = 0
	for res := range resultsChan {
		results[i] = res
		i++
	}

	return results
}

func FanOutStream(src io.Reader, count int) ([]io.ReadCloser, error) {
	type pipePair struct {
		Reader io.ReadCloser
		Writer io.WriteCloser
	}

	pipes := make([]pipePair, count)
	for i := range pipes {
		pr, pw := io.Pipe()
		pipes[i] = pipePair{Reader: pr, Writer: pw}
	}

	go func() {
		buf := make([]byte, 32*1024)

		for {
			n, err := src.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])

				// записываем всем пайпам последовательно
				for _, pipe := range pipes {
					_, writeErr := pipe.Writer.Write(chunk)
					if writeErr != nil {
						// логировать можно
					}
				}
			}

			if err != nil {
				for _, pipe := range pipes {
					pipe.Writer.Close()
				}
				break
			}
		}
	}()

	readers := make([]io.ReadCloser, count)
	for i, p := range pipes {
		readers[i] = p.Reader
	}
	return readers, nil
}
