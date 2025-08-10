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

type Availability struct {
	FileID   uuid.UUID `bson:"file_id"`
	FriendID uuid.UUID `bson:"friend_id"`
}

type AvailabilitiesRepositorier interface {
	Insert(availabilities ...*Availability) error
	GetByFileIDs(fileIDs ...uuid.UUID) []*Availability
	GetByFriendIDs(friendIDs ...uuid.UUID) []*Availability
	GetAll() []*Availability
	Get(fileID, friendID uuid.UUID) (*Availability, error)
	DeleteByFriendID(friendId uuid.UUID) error
	DeleteByFileID(fileId uuid.UUID) error
}

// in memory

type InMemoryAvailabilitiesRepository struct {
	mutex sync.RWMutex
	bd    []*Availability
}

func NewInMemoryAvailabilitiesRepository() *InMemoryAvailabilitiesRepository {
	return &InMemoryAvailabilitiesRepository{
		bd: make([]*Availability, 0),
	}
}

func copyAvailability(m *Availability) *Availability {
	t := *m
	return &t
}

func (r *InMemoryAvailabilitiesRepository) Insert(availabilities ...*Availability) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, v := range r.bd {
		for _, a := range availabilities {
			if v.FileID == a.FileID && v.FriendID == a.FriendID {
				return fmt.Errorf("availability already exists, friendID: %v, fileID: %v", v.FriendID, v.FileID)
			}
		}
	}

	for _, v := range availabilities {
		r.bd = append(r.bd, copyAvailability(v))
	}

	return nil
}

func (r *InMemoryAvailabilitiesRepository) GetByFileIDs(fileIDs ...uuid.UUID) []*Availability {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	result := make([]*Availability, 0)
	for _, v := range r.bd {
		for _, id := range fileIDs {
			if v.FileID == id {
				result = append(result, copyAvailability(v))
			}
		}
	}
	return result
}

func (r *InMemoryAvailabilitiesRepository) GetByFriendIDs(friendIDs ...uuid.UUID) []*Availability {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	result := make([]*Availability, 0)
	for _, v := range r.bd {
		for _, id := range friendIDs {
			if v.FriendID == id {
				result = append(result, copyAvailability(v))
			}
		}
	}
	return result
}

func (r *InMemoryAvailabilitiesRepository) GetAll() []*Availability {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.bd[:]
}

func (r *InMemoryAvailabilitiesRepository) Get(fileID, friendID uuid.UUID) (*Availability, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, v := range r.bd {
		if v.FileID == fileID && v.FriendID == friendID {
			return copyAvailability(v), nil
		}
	}
	return nil, fmt.Errorf("availability not found, friendID: %v, fileID: %v", friendID, fileID)
}

func (r *InMemoryAvailabilitiesRepository) DeleteByFriendID(friendId uuid.UUID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var new = make([]*Availability, 0)
	for _, v := range r.bd {
		if friendId != v.FriendID {
			new = append(new, v)
		}
	}

	r.bd = new
}

func (r *InMemoryAvailabilitiesRepository) DeleteByFileID(fileId uuid.UUID) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var newList []*Availability
	for _, v := range r.bd {
		if v.FileID != fileId {
			newList = append(newList, v)
		}
	}
	r.bd = newList

	return nil
}

// mongo (TODO: вынести в отдельный файл)

type MongoAvailabilitiesRepository struct {
	collection *mongo.Collection
	ctx        context.Context
}

func NewMongoAvailabilitiesRepository(connectionString string) (*MongoAvailabilitiesRepository, error) {
	const (
		dbName         string = "distorage"
		collectionName string = "availabilities"
	)

	ctx := context.Background()
	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %v", err)
	}

	collection := client.Database(dbName).Collection(collectionName)

	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "file_id", Value: 1},
			{Key: "friend_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, _ = collection.Indexes().CreateOne(ctx, index)

	return &MongoAvailabilitiesRepository{
		collection: collection,
		ctx:        ctx,
	}, nil
}

func (r *MongoAvailabilitiesRepository) Insert(availabilities ...*Availability) error {
	docs := make([]interface{}, len(availabilities))
	for i, a := range availabilities {
		docs[i] = a
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err := r.collection.InsertMany(r.ctx, docs, opts)

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("some availability already exists")
		}
		return err
	}
	return nil
}

func (r *MongoAvailabilitiesRepository) GetByFileIDs(fileIDs ...uuid.UUID) []*Availability {
	filter := bson.M{"file_id": bson.M{"$in": fileIDs}}
	return r.findMany(filter)
}

func (r *MongoAvailabilitiesRepository) GetByFriendIDs(friendIDs ...uuid.UUID) []*Availability {
	filter := bson.M{"friend_id": bson.M{"$in": friendIDs}}
	return r.findMany(filter)
}

func (r *MongoAvailabilitiesRepository) GetAll() []*Availability {
	return r.findMany(bson.M{})
}

func (r *MongoAvailabilitiesRepository) Get(fileID, friendID uuid.UUID) (*Availability, error) {
	filter := bson.M{"file_id": fileID, "friend_id": friendID}

	var result Availability
	err := r.collection.FindOne(r.ctx, filter).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("availability not found, fileID: %v, friendID: %v", fileID, friendID)
	}
	return &result, nil
}

func (r *MongoAvailabilitiesRepository) DeleteByFriendID(friendId uuid.UUID) error {
	_, err := r.collection.DeleteMany(r.ctx, bson.M{"friend_id": friendId})
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoAvailabilitiesRepository) DeleteByFileID(fileId uuid.UUID) error {
	_, err := r.collection.DeleteMany(r.ctx, bson.M{"file_id": fileId})
	if err != nil {
		return err
	}

	return nil
}

// Внутренний хелпер для выборок
func (r *MongoAvailabilitiesRepository) findMany(filter interface{}) []*Availability {
	cursor, err := r.collection.Find(r.ctx, filter)
	if err != nil {
		log.Println("findMany error:", err)
		return nil
	}
	defer cursor.Close(r.ctx)

	var results []*Availability
	for cursor.Next(r.ctx) {
		var a Availability
		if err := cursor.Decode(&a); err != nil {
			log.Println("decode error:", err)
			continue
		}
		results = append(results, &a)
	}
	return results
}
