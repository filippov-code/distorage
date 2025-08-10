package repositories

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Me struct {
	ID   uuid.UUID `bson:"_id"`
	URL  string    `bson:"url"`
	Port string    `bson:"port"`
}

type MeRepositorier interface {
	Get() *Me
}

// in memory

type InMemoryMeRepository struct {
	me *Me
}

func NewInMemoryMeRepository(url, port string) *InMemoryMeRepository {
	return &InMemoryMeRepository{
		me: &Me{ID: uuid.New(), URL: url, Port: port},
	}
}

func (r *InMemoryMeRepository) Get() *Me {
	t := *r.me
	return &t
}

// mongo (TODO: вынести в отдельный файл)

type MongoMeRepository struct {
	collection *mongo.Collection
	ctx        context.Context
}

// пример connectionString "mongodb://localhost:27017"
func NewMongoMeRepository(connectionString, url, port string) (*MongoMeRepository, error) {
	const (
		dbName         string = "distorage"
		collectionName string = "me"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(connectionString)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %v", err)
	}

	collection := client.Database(dbName).Collection(collectionName)

	//инициализация me
	me := &Me{
		ID:   uuid.New(),
		URL:  url,
		Port: port,
	}

	filter := bson.M{"_id": me.ID}
	update := bson.M{"$set": me}

	opts := options.Update().SetUpsert(true)
	backgroundCtx := context.Background()

	_, err = collection.UpdateOne(backgroundCtx, filter, update, opts)
	if err != nil {
		return nil, err
	}

	return &MongoMeRepository{
		collection: collection,
		ctx:        backgroundCtx,
	}, nil
}

func (r *MongoMeRepository) Get() *Me {
	var result Me
	err := r.collection.FindOne(r.ctx, bson.D{}).Decode(&result)
	if err != nil {
		log.Println("Get me failed:", err)
		return nil
	}

	return &result
}
