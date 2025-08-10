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

type Friend struct {
	ID   uuid.UUID `bson:"_id,omitempty"`
	Name string    `bson:"name"`
	URL  string    `bson:"url"`
}

type FriendsRepositorier interface {
	Insert(friends ...*Friend) error
	Get(id uuid.UUID) (*Friend, error)
	GetAll() []*Friend
	GetMany([]uuid.UUID) []*Friend
	Delete(friend uuid.UUID) error
}

// in memory

type InMemoryFriendsRepository struct {
	mutex sync.RWMutex
	bd    map[uuid.UUID]*Friend
}

func NewInMemoryFriendsRepository() *InMemoryFriendsRepository {
	return &InMemoryFriendsRepository{
		bd: make(map[uuid.UUID]*Friend),
	}
}

func copyFriend(m *Friend) *Friend {
	t := *m
	return &t
}

func (r *InMemoryFriendsRepository) Insert(friends ...*Friend) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, friend := range friends {
		if _, exists := r.bd[friend.ID]; exists {
			return fmt.Errorf("friend already exists, id: %v", friend.ID)
		}
	}

	for _, friend := range friends {
		r.bd[friend.ID] = copyFriend(friend)
	}

	return nil
}

func (r *InMemoryFriendsRepository) Get(id uuid.UUID) (*Friend, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	entity, exists := r.bd[id]
	if !exists {
		return nil, fmt.Errorf("friend not found, id: %v", id)
	}

	return copyFriend(entity), nil
}

func (r *InMemoryFriendsRepository) GetAll() []*Friend {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	all := make([]*Friend, 0, len(r.bd))
	for _, m := range r.bd {
		all = append(all, copyFriend(m))
	}
	return all
}

func (r *InMemoryFriendsRepository) GetMany(ids []uuid.UUID) []*Friend {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var hashmap = make(map[uuid.UUID]struct{})
	for _, id := range ids {
		hashmap[id] = struct{}{}
	}

	var res = make([]*Friend, 0, len(r.bd))
	for _, m := range r.bd {
		if _, exists := hashmap[m.ID]; exists {
			res = append(res, copyFriend(m))
		}
	}

	return res
}

func (r *InMemoryFriendsRepository) Delete(id uuid.UUID) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.bd[id]; !exists {
		return fmt.Errorf("friend not found, id: %v", id)
	}

	delete(r.bd, id)

	return nil
}

// mongo (TODO: вынести в отдельный файл)

type MongoFriendsRepository struct {
	collection *mongo.Collection
	ctx        context.Context
}

func NewMongoFriendsRepository(connectionString string) (*MongoFriendsRepository, error) {
	const (
		dbName         string = "distorage"
		collectionName string = "friends"
	)

	ctx := context.Background()
	clientOpts := options.Client().ApplyURI(connectionString)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %v", err)
	}

	collection := client.Database(dbName).Collection(collectionName)

	return &MongoFriendsRepository{
		collection: collection,
		ctx:        ctx,
	}, nil
}

func (r *MongoFriendsRepository) Insert(friends ...*Friend) error {
	if len(friends) == 0 {
		return nil
	}
	docs := make([]interface{}, len(friends))
	for i, friend := range friends {
		docs[i] = friend
	}

	_, err := r.collection.InsertMany(r.ctx, docs, options.InsertMany())
	if err != nil {
		return fmt.Errorf("failed to insert friends into MongoDB: %w", err)
	}

	return nil
}

func (r *MongoFriendsRepository) Get(id uuid.UUID) (*Friend, error) {
	var result Friend

	err := r.collection.FindOne(r.ctx, bson.M{"_id": id}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("friend not found, id: %v", id)
		}
		return nil, err
	}

	return &result, nil
}

func (r *MongoFriendsRepository) GetAll() []*Friend {
	cursor, err := r.collection.Find(r.ctx, bson.D{})
	if err != nil {
		log.Println("GetAll() failed:", err)
		return nil
	}
	defer cursor.Close(r.ctx)

	var results []*Friend
	for cursor.Next(r.ctx) {
		var f Friend
		if err := cursor.Decode(&f); err != nil {
			log.Println("Decode error:", err)
			continue
		}
		results = append(results, &f)
	}

	return results
}

func (r *MongoFriendsRepository) GetMany(ids []uuid.UUID) []*Friend {
	filter := bson.M{"_id": bson.M{"$in": ids}}

	cursor, err := r.collection.Find(r.ctx, filter)
	if err != nil {
		log.Println("GetMany() failed:", err)
		return nil
	}
	defer cursor.Close(r.ctx)

	var results []*Friend
	for cursor.Next(r.ctx) {
		var f Friend
		if err := cursor.Decode(&f); err != nil {
			log.Println("Decode error:", err)
			continue
		}
		results = append(results, &f)
	}

	return results
}

func (r *MongoFriendsRepository) Delete(id uuid.UUID) error {
	res, err := r.collection.DeleteOne(r.ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return fmt.Errorf("friend not found, id: %v", id)
	}
	return nil
}
