package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"time"

	gw "gateway/internal/grpc" // пакет с сгенерированным gRPC-клиентом

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	port    string
	nodeURL string
)

func main() {
	flag.StringVar(&port, "port", "", "Порт для запуска HTTP сервера")
	flag.StringVar(&nodeURL, "node", "", "Адрес ноды")
	flag.Parse()

	if port == "" {
		log.Fatal("port параметр обязателен")
	}
	if nodeURL == "" {
		log.Fatal("node параметр обязателен")
	}

	r := mux.NewRouter()
	r.HandleFunc("/files", handleUpload).Methods("POST")
	r.HandleFunc("/files/{id}", handleGet).Methods("GET")
	r.HandleFunc("/files/{id}", handleUpdate).Methods("PUT")
	r.HandleFunc("/files/{id}", handleDelete).Methods("DELETE")

	log.Printf("HTTP Gateway запущен на порту %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func grpcClientConn() (*grpc.ClientConn, gw.GatewayClient, error) {
	conn, err := grpc.NewClient(nodeURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc ошибка: %w", err)
	}
	client := gw.NewGatewayClient(conn)
	return conn, client, nil
}

// POST /upload
func handleUpload(w http.ResponseWriter, r *http.Request) {
	conn, client, err := grpcClientConn()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	stream, err := client.Upload(ctx)
	if err != nil {
		http.Error(w, "ошибка открытия grpc stream", http.StatusInternalServerError)
		return
	}

	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "некорректный multipart form", http.StatusBadRequest)
		return
	}

	firstPart := true
	buf := make([]byte, 32*1024)

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "ошибка четния multipart", http.StatusBadRequest)
			return
		}

		if part.FormName() != "file" {
			continue
		}

		// Первый пакет — только filename
		if firstPart {
			if err := stream.Send(&gw.UploadRequest{
				Filename: part.FileName(),
			}); err != nil {
				http.Error(w, "ошибка отправки", http.StatusInternalServerError)
				return
			}
			firstPart = false
		}

		// Передаём чанки
		for {
			n, readErr := part.Read(buf)
			if n > 0 {
				if err := stream.Send(&gw.UploadRequest{
					Chunk: buf[:n],
				}); err != nil {
					http.Error(w, "ошибка отправки чанка", http.StatusInternalServerError)
					return
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				http.Error(w, "ошибка чтения", http.StatusInternalServerError)
				return
			}
		}
	}

	// Закрываем стрим и ждём ответ
	resp, err := stream.CloseAndRecv()
	if err != nil {
		http.Error(w, "ошибка при получении ответа", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(resp.GetId()))
}

// GET /files/{id}
func handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	conn, client, err := grpcClientConn()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	stream, err := client.Get(ctx, &gw.GetRequest{Id: fileID})
	if err != nil {
		http.Error(w, "оишбка grpc get: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var filename string
	firstChunk := true

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "ошибка grpc stream recv: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if firstChunk {
			filename = msg.Filename
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(filename)))
			w.Header().Set("Content-Type", "application/octet-stream")
			firstChunk = false
		}

		if _, err := w.Write(msg.Chunk); err != nil {
			return
		}
	}
}

// PUT /files/{id}
func handleUpdate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	var body struct {
		Filename string `json:"filename"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "некорректный json", http.StatusBadRequest)
		return
	}
	if body.Filename == "" {
		http.Error(w, "filename обязателен", http.StatusBadRequest)
		return
	}

	conn, client, err := grpcClientConn()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Update(ctx, &gw.UpdateRequest{
		Id:       fileID,
		Filename: body.Filename,
	})
	if err != nil {
		http.Error(w, "ошибка grpc update: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DELETE /files/{id}
func handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	conn, client, err := grpcClientConn()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err = client.Delete(ctx, &gw.DeleteRequest{
		Id: fileID,
	})
	if err != nil {
		http.Error(w, "ошибка grpc delete: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
