// Package inbox provides transactional idempotency for event consumers.
package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
)

const savepoint = "inbox_domain_write"

var validTableName = regexp.MustCompile(`^(?:[a-z][a-z0-9_]{0,62}\.)?[a-z][a-z0-9_]{0,62}$`)

// Dialect selects the small amount of SQL that differs between supported databases.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectKingbase Dialect = "kingbase"
	DialectMySQL    Dialect = "mysql"
)

// Key uniquely identifies one event for one durable consumer.
type Key struct {
	Consumer string
	EventID  string
}

// Result describes the persisted delivery state.
type Result struct {
	Duplicate bool
	Attempts  int
}

// Handler performs domain writes on the same transaction as the Inbox record.
// It must not commit or roll back tx.
type Handler func(ctx context.Context, tx *sqlx.Tx) error

// SQLStore coordinates an Inbox record and domain writes in one local transaction.
type SQLStore struct {
	db      *sqlx.DB
	dialect Dialect
	table   string
	now     func() time.Time
}

// NewSQLStore constructs a store. table may be schema-qualified, but every
// identifier must use lower-case ASCII letters, numbers, and underscores.
func NewSQLStore(db *sqlx.DB, dialect Dialect, table string) (*SQLStore, error) {
	if db == nil {
		return nil, errors.New("inbox database is required")
	}
	if !validTableName.MatchString(table) {
		return nil, errors.New("safe inbox table name is required")
	}
	if dialect != DialectPostgres && dialect != DialectKingbase && dialect != DialectMySQL {
		return nil, fmt.Errorf("unsupported inbox dialect %q", dialect)
	}
	return &SQLStore{db: db, dialect: dialect, table: table, now: time.Now}, nil
}

// Process runs handler at most once successfully for key. A failed handler's
// domain writes are rolled back to a savepoint while its failed attempt is
// committed for diagnostics. The returned error still asks the broker to retry.
func (s *SQLStore) Process(ctx context.Context, key Key, actor string, handler Handler) (Result, error) {
	if err := validateInput(key, actor, handler); err != nil {
		return Result{}, err
	}

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return Result{}, fmt.Errorf("begin inbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := s.now().In(time.FixedZone("UTC+8", 8*60*60))
	if err := s.insertPending(ctx, tx, key, actor, now); err != nil {
		return Result{}, err
	}

	status, attempts, err := s.lock(ctx, tx, key)
	if err != nil {
		return Result{}, err
	}
	if status == "completed" {
		if err := tx.Commit(); err != nil {
			return Result{}, fmt.Errorf("commit duplicate inbox transaction: %w", err)
		}
		return Result{Duplicate: true, Attempts: attempts}, nil
	}

	attempts++
	if err := s.markProcessing(ctx, tx, key, actor, now); err != nil {
		return Result{}, err
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return Result{}, fmt.Errorf("create inbox savepoint: %w", err)
	}

	if handlerErr := handler(ctx, tx); handlerErr != nil {
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
			return Result{}, errors.Join(handlerErr, fmt.Errorf("rollback inbox domain writes: %w", err))
		}
		if err := s.markFailed(ctx, tx, key, actor, handlerErr, now); err != nil {
			return Result{}, errors.Join(handlerErr, err)
		}
		if err := tx.Commit(); err != nil {
			return Result{}, errors.Join(handlerErr, fmt.Errorf("commit failed inbox attempt: %w", err))
		}
		return Result{Attempts: attempts}, fmt.Errorf("process inbox event: %w", handlerErr)
	}

	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return Result{}, fmt.Errorf("release inbox savepoint: %w", err)
	}
	if err := s.markCompleted(ctx, tx, key, actor, now); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, fmt.Errorf("commit inbox transaction: %w", err)
	}
	return Result{Attempts: attempts}, nil
}

// DeleteCompletedBefore removes only successfully completed inbox records in a
// bounded batch. Schedule it with a retention horizon at least as long as the
// corresponding event stream can replay messages.
func (s *SQLStore) DeleteCompletedBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, errors.New("inbox cleanup limit must be positive")
	}
	var keys []struct {
		Consumer string `db:"consumer"`
		EventID  string `db:"event_id"`
	}
	selectQuery := s.db.Rebind(`SELECT consumer,event_id FROM ` + s.table + ` WHERE status='completed' AND completed_at<? ORDER BY completed_at,consumer,event_id LIMIT ?`)
	if err := s.db.SelectContext(ctx, &keys, selectQuery, before, limit); err != nil || len(keys) == 0 {
		return 0, err
	}
	var deleted int64
	for _, key := range keys {
		query := s.db.Rebind(`DELETE FROM ` + s.table + ` WHERE consumer=? AND event_id=? AND status='completed' AND completed_at<?`)
		result, err := s.db.ExecContext(ctx, query, key.Consumer, key.EventID, before)
		if err != nil {
			return deleted, fmt.Errorf("delete completed inbox event: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return deleted, fmt.Errorf("count deleted inbox event: %w", err)
		}
		deleted += count
	}
	return deleted, nil
}

func validateInput(key Key, actor string, handler Handler) error {
	if key.Consumer == "" || key.EventID == "" || actor == "" || handler == nil {
		return errors.New("inbox consumer, event ID, actor, and handler are required")
	}
	if len(key.Consumer) > 255 || len(key.EventID) > 255 || len(actor) > 255 {
		return errors.New("inbox consumer, event ID, and actor must not exceed 255 bytes")
	}
	return nil
}

func (s *SQLStore) insertPending(ctx context.Context, tx *sqlx.Tx, key Key, actor string, now time.Time) error {
	query := s.insertPendingQuery()
	if _, err := tx.ExecContext(ctx, s.db.Rebind(query), key.Consumer, key.EventID, "pending", now, now, actor, actor); err != nil {
		return fmt.Errorf("insert pending inbox event: %w", err)
	}
	return nil
}

func (s *SQLStore) insertPendingQuery() string {
	query := `INSERT INTO ` + s.table + ` (consumer,event_id,status,attempts,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,'',1,?,?,?,?)`
	if s.dialect == DialectMySQL {
		return "INSERT IGNORE" + query[len("INSERT"):]
	}
	return query + " ON CONFLICT (consumer,event_id) DO NOTHING"
}

func (s *SQLStore) lock(ctx context.Context, tx *sqlx.Tx, key Key) (string, int, error) {
	var row struct {
		Status   string `db:"status"`
		Attempts int    `db:"attempts"`
	}
	query := s.db.Rebind(`SELECT status,attempts FROM ` + s.table + ` WHERE consumer=? AND event_id=? FOR UPDATE`)
	if err := tx.GetContext(ctx, &row, query, key.Consumer, key.EventID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, errors.New("pending inbox event disappeared")
		}
		return "", 0, fmt.Errorf("lock inbox event: %w", err)
	}
	return row.Status, row.Attempts, nil
}

func (s *SQLStore) markProcessing(ctx context.Context, tx *sqlx.Tx, key Key, actor string, now time.Time) error {
	query := s.db.Rebind(`UPDATE ` + s.table + ` SET status='processing',attempts=attempts+1,last_error='',version=version+1,updated_at=?,updated_by=? WHERE consumer=? AND event_id=?`)
	return execOne(ctx, tx, query, "mark inbox event processing", now, actor, key.Consumer, key.EventID)
}

func (s *SQLStore) markFailed(ctx context.Context, tx *sqlx.Tx, key Key, actor string, cause error, now time.Time) error {
	message := truncate(cause.Error(), 4096)
	query := s.db.Rebind(`UPDATE ` + s.table + ` SET status='failed',last_error=?,version=version+1,updated_at=?,updated_by=? WHERE consumer=? AND event_id=?`)
	return execOne(ctx, tx, query, "mark inbox event failed", message, now, actor, key.Consumer, key.EventID)
}

func (s *SQLStore) markCompleted(ctx context.Context, tx *sqlx.Tx, key Key, actor string, now time.Time) error {
	query := s.db.Rebind(`UPDATE ` + s.table + ` SET status='completed',last_error='',completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE consumer=? AND event_id=?`)
	return execOne(ctx, tx, query, "mark inbox event completed", now, now, actor, key.Consumer, key.EventID)
}

func execOne(ctx context.Context, tx *sqlx.Tx, query, action string, args ...any) error {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count affected rows after %s: %w", action, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: expected one affected row, got %d", action, count)
	}
	return nil
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && value[limit]&0xc0 == 0x80 {
		limit--
	}
	return value[:limit]
}
