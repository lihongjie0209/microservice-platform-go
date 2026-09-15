package outbox

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
)

func TestNewSQLStoreRejectsInjectedTableName(t *testing.T) {
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := NewSQLStore(sqlx.NewDb(database, "sqlmock"), "events; DROP TABLE users"); err == nil {
		t.Fatal("unsafe table name accepted")
	}
}

func TestNewSQLStoreValidatesWorkerAuditActor(t *testing.T) {
	t.Parallel()
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := NewSQLStore(sqlx.NewDb(database, "sqlmock"), "events", WithWorkerAuditActor("  ")); err == nil {
		t.Fatal("blank worker actor accepted")
	}
}

func TestSQLStoreMarkPublishedInjectsMySQLWorkerActor(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "mysql")
	store, err := NewSQLStore(db, "application_outbox_events", WithWorkerAuditActor("application-service:outbox"))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET @app_actor_id = ?")).WithArgs("application-service:outbox").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE application_outbox_events SET published_at=?,version=version+1,updated_at=?,updated_by='outbox-dispatcher',last_error='' WHERE id=? AND published_at IS NULL")).WithArgs(at, at, "event-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.MarkPublished(t.Context(), Event{ID: "event-1"}, at); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreInjectsPostgresWorkerActor(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "pgx")
	store, err := NewSQLStore(db, "application_outbox_events", WithWorkerAuditActor("application-service:outbox"))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	tx, err := db.BeginTxx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config('app.actor_id', $1, true)")).WithArgs("application-service:outbox").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.setWorkerAuditActor(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreAddTxPersistsEnvelopeInCallerTransaction(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	store, err := NewSQLStore(db, "workflow_outbox_events")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	mock.ExpectBegin()
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO workflow_outbox_events (id,subject,envelope,attempts,available_at,published_at,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,?,NULL,'',1,?,?,?,?)")).
		WithArgs("event-1", "platform.workflow.instance.requested.v1", sqlmock.AnyArg(), now, now, now, "actor-1", "actor-1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = store.AddTx(context.Background(), tx, Event{ID: "event-1", Subject: "platform.workflow.instance.requested.v1", Envelope: &commonv1.EventEnvelope{EventId: "event-1", EventType: "platform.workflow.instance.requested.v1"}}, "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreAddTxRejectsEnvelopeMismatch(t *testing.T) {
	t.Parallel()
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	store, err := NewSQLStore(db, "workflow_outbox_events")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddTx(context.Background(), &sqlx.Tx{}, Event{ID: "outer", Subject: "subject", Envelope: &commonv1.EventEnvelope{EventId: "inner", EventType: "subject"}}, "actor"); err == nil {
		t.Fatal("AddTx() error = nil")
	}
}

func TestSQLStoreDeletePublishedBeforeDeletesBoundedTerminalEvents(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := NewSQLStore(sqlx.NewDb(database, "sqlmock"), "workflow_outbox_events")
	if err != nil {
		t.Fatal(err)
	}
	before := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM workflow_outbox_events WHERE published_at IS NOT NULL AND published_at<? ORDER BY published_at,id LIMIT ?")).
		WithArgs(before, 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("event-1").AddRow("event-2"))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM workflow_outbox_events WHERE id IN (?, ?) AND published_at IS NOT NULL AND published_at<?")).
		WithArgs("event-1", "event-2", before).
		WillReturnResult(sqlmock.NewResult(0, 2))

	deleted, err := store.DeletePublishedBefore(t.Context(), before, 2)
	if err != nil {
		t.Fatalf("DeletePublishedBefore() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreDeletePublishedBeforeRejectsInvalidLimit(t *testing.T) {
	t.Parallel()
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := NewSQLStore(sqlx.NewDb(database, "sqlmock"), "workflow_outbox_events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeletePublishedBefore(t.Context(), time.Now(), 0); err == nil {
		t.Fatal("DeletePublishedBefore() error = nil")
	}
}
