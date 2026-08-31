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

// AddTx persists an event inside the caller's business transaction. The event
// is not visible to a dispatcher until that transaction commits.
func (s *SQLStore) AddTx(ctx context.Context, tx *sqlx.Tx, event Event, actor string) error {
	if tx == nil || event.ID == "" || event.Subject == "" || event.Envelope == nil || actor == "" {
		return errors.New("transaction, complete event, and audit actor are required")
	}
	if event.Envelope.GetEventId() != event.ID || event.Envelope.GetEventType() != event.Subject {
		return errors.New("outbox event id and subject must match its envelope")
	}
	encoded, err := proto.Marshal(event.Envelope)
	if err != nil {
		return fmt.Errorf("encode %s event %q: %w", s.table, event.ID, err)
	}
	now := s.now()
	query := s.db.Rebind(`INSERT INTO ` + s.table + ` (id,subject,envelope,attempts,available_at,published_at,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,?,NULL,'',1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, query, event.ID, event.Subject, encoded, now, now, now, actor, actor); err != nil {
		return fmt.Errorf("insert %s event %q: %w", s.table, event.ID, err)
	}
	return nil
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

// DeletePublishedBefore removes only terminal, successfully published events in
// a bounded batch. Callers decide their own retention period and archival
// policy; pending and failed events are never eligible for cleanup.
func (s *SQLStore) DeletePublishedBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, errors.New("outbox cleanup limit must be positive")
	}
	var ids []string
	selectQuery := s.db.Rebind(`SELECT id FROM ` + s.table + ` WHERE published_at IS NOT NULL AND published_at<? ORDER BY published_at,id LIMIT ?`)
	if err := s.db.SelectContext(ctx, &ids, selectQuery, before, limit); err != nil || len(ids) == 0 {
		return 0, err
	}
	deleteQuery, args, err := sqlx.In(`DELETE FROM `+s.table+` WHERE id IN (?) AND published_at IS NOT NULL AND published_at<?`, ids, before)
	if err != nil {
		return 0, fmt.Errorf("build %s cleanup query: %w", s.table, err)
	}
	result, err := s.db.ExecContext(ctx, s.db.Rebind(deleteQuery), args...)
	if err != nil {
		return 0, fmt.Errorf("delete published %s events: %w", s.table, err)
	}
	return result.RowsAffected()
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
