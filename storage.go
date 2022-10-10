package smp

import (
	"context"
	"github.com/rumorsflow/mongo-ext"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type PeriodicTaskStorage interface {
	Find(ctx context.Context, criteria mongoext.Criteria) ([]PeriodicTask, error)
	Count(ctx context.Context, filter any) (int64, error)
	FindById(ctx context.Context, id string) (PeriodicTask, error)
	Save(ctx context.Context, model *PeriodicTask) error
	Delete(ctx context.Context, id string) error
}

type storage struct {
	c *mongo.Collection
}

func newStorage(db *mongo.Database) *storage {
	return &storage{c: db.Collection("scheduler")}
}

func (s *storage) indexes(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, mongoext.Timeout)
	defer cancel()

	_, err := s.c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{"job_code", 1},
			{"payload", 1},
			{"created_at", 1},
			{"updated_at", 1},
			{"enabled", 1},
		},
	})

	return err
}

func (s *storage) Find(ctx context.Context, criteria mongoext.Criteria) ([]PeriodicTask, error) {
	return mongoext.FindByCriteria[PeriodicTask](ctx, s.c, criteria)
}

func (s *storage) Count(ctx context.Context, filter any) (int64, error) {
	return mongoext.Count(ctx, s.c, filter)
}

func (s *storage) FindById(ctx context.Context, id string) (PeriodicTask, error) {
	return mongoext.FindOne[PeriodicTask](ctx, s.c, bson.D{{"_id", id}})
}

func (s *storage) Save(ctx context.Context, model *PeriodicTask) error {
	now := time.Now().UTC()
	model.UpdatedAt = now

	data, err := mongoext.ToBson(model)
	if err != nil {
		return err
	}

	delete(data, "_id")
	delete(data, "created_at")

	result, err := mongoext.Save(ctx, s.c, bson.D{{"_id", model.Id}}, bson.M{
		"$set": data,
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	})
	if err != nil {
		return err
	}

	if result.UpsertedCount > 0 {
		model.CreatedAt = now
	}

	return nil
}

func (s *storage) Delete(ctx context.Context, id string) error {
	return mongoext.Delete(ctx, s.c, bson.D{{"_id", id}})
}
