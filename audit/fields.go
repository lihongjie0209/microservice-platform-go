// Package audit defines the platform persistence metadata passed explicitly to
// repositories. Database triggers must not guess the authenticated actor.
package audit

import (
	"context"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type Fields struct {
	Version   int64     `db:"version" json:"version"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	CreatedBy string    `db:"created_by" json:"created_by"`
	UpdatedBy string    `db:"updated_by" json:"updated_by"`
}

func New(ctx context.Context, now time.Time) (Fields, error) {
	actor, err := principal.Require(ctx)
	if err != nil {
		return Fields{}, err
	}
	return Fields{
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
		CreatedBy: actor.ID,
		UpdatedBy: actor.ID,
	}, nil
}

func UpdatedBy(ctx context.Context, now time.Time) (string, time.Time, error) {
	actor, err := principal.Require(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	return actor.ID, now, nil
}
