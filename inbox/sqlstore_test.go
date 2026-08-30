package inbox

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestNewSQLStoreValidatesDependencies(t *testing.T) {
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")

	tests := []struct {
		name    string
		db      *sqlx.DB
		dialect Dialect
		table   string
	}{
		{name: "nil database", dialect: DialectPostgres, table: "event_inbox"},
		{name: "injected table", db: db, dialect: DialectPostgres, table: "inbox;drop table users"},
		{name: "too many qualifiers", db: db, dialect: DialectPostgres, table: "db.schema.inbox"},
		{name: "unsupported dialect", db: db, dialect: "sqlite", table: "event_inbox"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSQLStore(test.db, test.dialect, test.table); err == nil {
				t.Fatal("NewSQLStore() accepted invalid input")
			}
		})
	}
}

func TestSQLStoreProcessSuccessAndDuplicate(t *testing.T) {
	store, mock := newMockStore(t)
	key := Key{Consumer: "users_projection", EventID: "event-1"}

	expectPendingAndLock(mock, key, "pending", 0)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE event_inbox SET status='processing',attempts=attempts+1,last_error='',version=version+1,updated_at=?,updated_by=? WHERE consumer=? AND event_id=?")).
		WithArgs(sqlmock.AnyArg(), "users_projection", key.Consumer, key.EventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SAVEPOINT inbox_domain_write").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO user_projection (id) VALUES (?)")).
		WithArgs("user-1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("RELEASE SAVEPOINT inbox_domain_write").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE event_inbox SET status='completed',last_error='',completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE consumer=? AND event_id=?")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "users_projection", key.Consumer, key.EventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.Process(context.Background(), key, "users_projection", func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO user_projection (id) VALUES (?)", "user-1")
		return err
	})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Duplicate || result.Attempts != 1 {
		t.Fatalf("Process() result = %+v", result)
	}

	expectPendingAndLock(mock, key, "completed", 1)
	mock.ExpectCommit()
	called := false
	result, err = store.Process(context.Background(), key, "users_projection", func(context.Context, *sqlx.Tx) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("duplicate Process() error = %v", err)
	}
	if !result.Duplicate || result.Attempts != 1 || called {
		t.Fatalf("duplicate Process() result = %+v, called = %v", result, called)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreProcessRollsBackDomainWritesAndRecordsFailure(t *testing.T) {
	store, mock := newMockStore(t)
	key := Key{Consumer: "users_projection", EventID: "event-2"}
	wantErr := errors.New("temporary upstream failure")

	expectPendingAndLock(mock, key, "failed", 2)
	mock.ExpectExec("UPDATE event_inbox SET status='processing'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SAVEPOINT inbox_domain_write").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ROLLBACK TO SAVEPOINT inbox_domain_write").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE event_inbox SET status='failed'").
		WithArgs(wantErr.Error(), sqlmock.AnyArg(), "users_projection", key.Consumer, key.EventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.Process(context.Background(), key, "users_projection", func(context.Context, *sqlx.Tx) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want wrapped %v", err, wantErr)
	}
	if result.Attempts != 3 || result.Duplicate {
		t.Fatalf("Process() result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreProcessValidatesKeyBeforeTransaction(t *testing.T) {
	store, mock := newMockStore(t)
	_, err := store.Process(context.Background(), Key{}, "", nil)
	if err == nil {
		t.Fatal("Process() accepted invalid input")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStoreInsertPendingQueryMatchesContract(t *testing.T) {
	store, _ := newMockStore(t)
	want := "INSERT INTO event_inbox (consumer,event_id,status,attempts,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,'',1,?,?,?,?) ON CONFLICT (consumer,event_id) DO NOTHING"
	if got := store.insertPendingQuery(); got != want {
		t.Fatalf("insertPendingQuery() = %q", got)
	}
	store.dialect = DialectMySQL
	want = "INSERT IGNORE INTO event_inbox (consumer,event_id,status,attempts,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,'',1,?,?,?,?)"
	if got := store.insertPendingQuery(); got != want {
		t.Fatalf("MySQL insertPendingQuery() = %q", got)
	}
}

func TestTruncatePreservesUTF8(t *testing.T) {
	value := "错误错误"
	got := truncate(value, 7)
	if got != "错误" {
		t.Fatalf("truncate() = %q", got)
	}
}

func newMockStore(t *testing.T) (*SQLStore, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := NewSQLStore(sqlx.NewDb(database, "sqlmock"), DialectPostgres, "event_inbox")
	if err != nil {
		t.Fatal(err)
	}
	return store, mock
}

func expectPendingAndLock(mock sqlmock.Sqlmock, key Key, status string, attempts int) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO event_inbox (consumer,event_id,status,attempts,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,'',1,?,?,?,?) ON CONFLICT (consumer,event_id) DO NOTHING")).
		WithArgs(key.Consumer, key.EventID, "pending", sqlmock.AnyArg(), sqlmock.AnyArg(), "users_projection", "users_projection").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT status,attempts FROM event_inbox WHERE consumer=? AND event_id=? FOR UPDATE")).
		WithArgs(key.Consumer, key.EventID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).AddRow(status, attempts))
}
