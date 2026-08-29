package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"google.golang.org/protobuf/proto"
)

var validTable = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type SQLStore struct {
	db    *sqlx.DB
	table string
	now   func() time.Time
}

func NewSQLStore(db *sqlx.DB, table string) (*SQLStore, error) {
	if db == nil || !validTable.MatchString(table) {
		return nil, errors.New("database and a safe outbox table name are required")
	}
	return &SQLStore{db: db, table: table, now: time.Now}, nil
}

func (s *SQLStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]Event, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin %s claim: %w", s.table, err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.now()
	var rows []struct {
		ID       string `db:"id"`
		Subject  string `db:"subject"`
		Envelope []byte `db:"envelope"`
	}
	query := s.db.Rebind(`SELECT id,subject,envelope FROM ` + s.table + ` WHERE published_at IS NULL AND available_at<=? ORDER BY available_at,created_at LIMIT ? FOR UPDATE SKIP LOCKED`)
	if err := tx.SelectContext(ctx, &rows, query, now, limit); err != nil {
		return nil, fmt.Errorf("select %s: %w", s.table, err)
	}
	values := make([]Event, 0, len(rows))
	for _, row := range rows {
		envelope := new(commonv1.EventEnvelope)
		if err := proto.Unmarshal(row.Envelope, envelope); err != nil {
			return nil, fmt.Errorf("decode %s event %q: %w", s.table, row.ID, err)
		}
		update := s.db.Rebind(`UPDATE ` + s.table + ` SET attempts=attempts+1,available_at=?,version=version+1,updated_at=?,updated_by='outbox-dispatcher' WHERE id=? AND published_at IS NULL`)
		if _, err := tx.ExecContext(ctx, update, now.Add(lease), now, row.ID); err != nil {
			return nil, fmt.Errorf("lease %s event %q: %w", s.table, row.ID, err)
		}
		values = append(values, Event{ID: row.ID, Subject: row.Subject, Envelope: envelope})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit %s claim: %w", s.table, err)
	}
	return values, nil
}

func (s *SQLStore) MarkPublished(ctx context.Context, event Event, at time.Time) error {
	query := s.db.Rebind(`UPDATE ` + s.table + ` SET published_at=?,version=version+1,updated_at=?,updated_by='outbox-dispatcher',last_error='' WHERE id=? AND published_at IS NULL`)
	result, err := s.db.ExecContext(ctx, query, at, at, event.ID)
	return affected(result, err, s.table, event.ID)
}

func (s *SQLStore) MarkFailed(ctx context.Context, event Event, message string, retryAt time.Time) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	query := s.db.Rebind(`UPDATE ` + s.table + ` SET available_at=?,last_error=?,version=version+1,updated_at=?,updated_by='outbox-dispatcher' WHERE id=? AND published_at IS NULL`)
	result, err := s.db.ExecContext(ctx, query, retryAt, message, s.now(), event.ID)
	return affected(result, err, s.table, event.ID)
}

func affected(result sql.Result, err error, table, id string) error {
	if err != nil {
		return fmt.Errorf("update %s event %q: %w", table, id, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count %s event %q: %w", table, id, err)
	}
	if count != 1 {
		return fmt.Errorf("%s event %q is no longer pending", table, id)
	}
	return nil
}
