package repositories

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Metadata struct {
	ID          uuid.UUID `bson:"_id"` //Совпадает с Id файла
	Filename    string    `bson:"filename"`
	IsAvailable bool      `bson:"is_available"` //Признак наличия файла на текущем узле
}

type MetadataRepositorier interface {
	Insert(params ...*Metadata) error
	Get(id uuid.UUID) (*Metadata, error)
	GetAll() []*Metadata
	GetAvailable() []*Metadata
	SetIsAvailable(id uuid.UUID, IsAvailable bool) error
	Update(id uuid.UUID, newFilename string) error
	Delete(id uuid.UUID) error
}

// in memory

type InMemoryMetadataRepository struct {
	mutex sync.RWMutex
	bd    map[uuid.UUID]*Metadata
}

func NewInMemoryMetadataRepository() *InMemoryMetadataRepository {
	return &InMemoryMetadataRepository{
		bd: make(map[uuid.UUID]*Metadata),
	}
}

func copyMetadata(m *Metadata) *Metadata {
	t := *m
	return &t
}

func (r *InMemoryMetadataRepository) Insert(metadatas ...*Metadata) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, metadata := range metadatas {
		if _, exists := r.bd[metadata.ID]; exists {
			return fmt.Errorf("metadata already exists, id: %v", metadata.ID)
		}
	}

	for _, metadata := range metadatas {
		r.bd[metadata.ID] = copyMetadata(metadata)
	}

	return nil
}

func (r *InMemoryMetadataRepository) Get(id uuid.UUID) (*Metadata, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	entity, exists := r.bd[id]
	if !exists {
		return nil, fmt.Errorf("metadata not found, id: %v", id)
	}

	return copyMetadata(entity), nil
}

func (r *InMemoryMetadataRepository) GetMany(ids []uuid.UUID) ([]*Metadata, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var res = make([]*Metadata, len(ids))

	for _, id := range ids {
		entity, exists := r.bd[id]
		if !exists {
			return nil, fmt.Errorf("metadata not found, id: %v", id)
		}

		res = append(res, copyMetadata(entity))
	}

	return res, nil
}

func (r *InMemoryMetadataRepository) GetAll() []*Metadata {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	all := make([]*Metadata, 0, len(r.bd))
	for _, m := range r.bd {
		all = append(all, copyMetadata(m))
	}
	return all
}

func (r *InMemoryMetadataRepository) GetAvailable() []*Metadata {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	availables := make([]*Metadata, 0, len(r.bd))
	for _, m := range r.bd {
		if m.IsAvailable {
			availables = append(availables, copyMetadata(m))
		}
	}
	return availables
}

func (r *InMemoryMetadataRepository) SetIsAvailable(id uuid.UUID, isAvailable bool) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.bd[id]; !exists {
		return fmt.Errorf("metadata not found, id: %v", id)
	}

	r.bd[id].IsAvailable = isAvailable

	return nil
}

func (r *InMemoryMetadataRepository) Delete(id uuid.UUID) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.bd, id)

	return nil
}

// mongo (TODO: вынести в отдельный файл)

type MongoMetadataRepository struct {
	collection *mongo.Collection
	ctx        context.Context
}

func NewMongoMetadataRepository(connectionString string) (*MongoMetadataRepository, error) {
	const (
		dbName         string = "distorage"
		collectionName string = "metadata"
	)

	ctx := context.Background()
	clientOpts := options.Client().ApplyURI(connectionString)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %v", err)
	}

	collection := client.Database(dbName).Collection(collectionName)

	_, _ = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "is_available", Value: 1}},
	})

	return &MongoMetadataRepository{
		collection: collection,
		ctx:        ctx,
	}, nil
}

func (r *MongoMetadataRepository) Insert(metadatas ...*Metadata) error {
	docs := make([]interface{}, len(metadatas))
	for i, m := range metadatas {
		docs[i] = m
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err := r.collection.InsertMany(r.ctx, docs, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("some metadata already exists")
		}
		return err
	}
	return nil
}

func (r *MongoMetadataRepository) Get(id uuid.UUID) (*Metadata, error) {
	var result Metadata
	err := r.collection.FindOne(r.ctx, bson.M{"_id": id}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("metadata not found, id: %v", id)
	}
	return &result, nil
}

func (r *MongoMetadataRepository) GetMany(ids []uuid.UUID) ([]*Metadata, error) {
	filter := bson.M{"_id": bson.M{"$in": ids}}
	cursor, err := r.collection.Find(r.ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(r.ctx)

	var results []*Metadata
	for cursor.Next(r.ctx) {
		var m Metadata
		if err := cursor.Decode(&m); err != nil {
			continue
		}
		results = append(results, &m)
	}

	if len(results) != len(ids) {
		return nil, fmt.Errorf("some metadata not found")
	}

	return results, nil
}

func (r *MongoMetadataRepository) GetAll() []*Metadata {
	cursor, err := r.collection.Find(r.ctx, bson.M{})
	if err != nil {
		log.Println("GetAll failed:", err)
		return nil
	}
	defer cursor.Close(r.ctx)

	var results []*Metadata
	for cursor.Next(r.ctx) {
		var m Metadata
		if err := cursor.Decode(&m); err != nil {
			continue
		}
		results = append(results, &m)
	}
	return results
}

func (r *MongoMetadataRepository) GetAvailable() []*Metadata {
	cursor, err := r.collection.Find(r.ctx, bson.M{"is_available": true})
	if err != nil {
		log.Println("GetAvailable failed:", err)
		return nil
	}
	defer cursor.Close(r.ctx)

	var results []*Metadata
	for cursor.Next(r.ctx) {
		var m Metadata
		if err := cursor.Decode(&m); err != nil {
			continue
		}
		results = append(results, &m)
	}
	return results
}

func (r *MongoMetadataRepository) SetIsAvailable(id uuid.UUID, isAvailable bool) error {
	res, err := r.collection.UpdateOne(r.ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"is_available": isAvailable}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("metadata not found, id: %v", id)
	}
	return nil
}

func (r *MongoMetadataRepository) Update(id uuid.UUID, newFilename string) error {
	res, err := r.collection.UpdateOne(r.ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"filename": newFilename}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("metadata not found, id: %v", id)
	}
	return nil
}

func (r *MongoMetadataRepository) Delete(id uuid.UUID) error {
	_, err := r.collection.DeleteMany(r.ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}

	return nil
}
