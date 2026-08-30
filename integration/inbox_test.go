//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/inbox"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestInboxPostgresAndMySQL(t *testing.T) {
	tests := []struct {
		name         string
		image        string
		port         string
		driver       string
		dialect      inbox.Dialect
		env          map[string]string
		dsn          func(host, port string) string
		createSchema string
		table        string
	}{
		{
			name: "postgres", image: "postgres:17-alpine", port: "5432/tcp", driver: "pgx", dialect: inbox.DialectPostgres,
			env: map[string]string{"POSTGRES_USER": "platform", "POSTGRES_PASSWORD": "platform", "POSTGRES_DB": "platform"},
			dsn: func(host, port string) string {
				return fmt.Sprintf("postgres://platform:platform@%s:%s/platform?sslmode=disable", host, port)
			},
			createSchema: postgresInboxSchema,
			table:        "inbox_test.event_inbox",
		},
		{
			name: "mysql", image: "mysql:8.4", port: "3306/tcp", driver: "mysql", dialect: inbox.DialectMySQL,
			env: map[string]string{"MYSQL_ROOT_PASSWORD": "platform", "MYSQL_DATABASE": "platform"},
			dsn: func(host, port string) string {
				return fmt.Sprintf("root:platform@tcp(%s:%s)/platform?parseTime=true&multiStatements=true", host, port)
			},
			createSchema: mysqlInboxSchema,
			table:        "event_inbox",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			db := startInboxDatabase(t, ctx, test.image, test.port, test.driver, test.env, test.dsn)
			if _, err := db.ExecContext(ctx, test.createSchema); err != nil {
				t.Fatalf("create Inbox fixture: %v", err)
			}
			store, err := inbox.NewSQLStore(db, test.dialect, test.table)
			if err != nil {
				t.Fatal(err)
			}
			assertConcurrentInboxDeduplication(t, ctx, db, store)
			assertInboxFailureRollbackAndRetry(t, ctx, db, store)
		})
	}
}

func assertConcurrentInboxDeduplication(t *testing.T, ctx context.Context, db *sqlx.DB, store *inbox.SQLStore) {
	t.Helper()
	key := inbox.Key{Consumer: "concurrent_projection", EventID: "event-concurrent"}
	var calls atomic.Int32
	start := make(chan struct{})
	results := make(chan inbox.Result, 2)
	errorsFound := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			result, err := store.Process(ctx, key, "integration-test", func(ctx context.Context, tx *sqlx.Tx) error {
				calls.Add(1)
				_, err := tx.ExecContext(ctx, db.Rebind("INSERT INTO inbox_effects (id,value) VALUES (?,?)"), "effect-concurrent", "created")
				return err
			})
			results <- result
			errorsFound <- err
		}()
	}
	ready.Wait()
	close(start)
	duplicates := 0
	for range 2 {
		if err := <-errorsFound; err != nil {
			t.Fatalf("concurrent Process(): %v", err)
		}
		if (<-results).Duplicate {
			duplicates++
		}
	}
	if calls.Load() != 1 || duplicates != 1 {
		t.Fatalf("handler calls = %d, duplicates = %d", calls.Load(), duplicates)
	}
	var effects int
	if err := db.GetContext(ctx, &effects, "SELECT COUNT(*) FROM inbox_effects WHERE id='effect-concurrent'"); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("domain effects = %d", effects)
	}
}

func assertInboxFailureRollbackAndRetry(t *testing.T, ctx context.Context, db *sqlx.DB, store *inbox.SQLStore) {
	t.Helper()
	key := inbox.Key{Consumer: "retry_projection", EventID: "event-retry"}
	wantErr := errors.New("retry this delivery")
	result, err := store.Process(ctx, key, "integration-test", func(ctx context.Context, tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, db.Rebind("INSERT INTO inbox_effects (id,value) VALUES (?,?)"), "effect-retry", "must-roll-back"); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) || result.Attempts != 1 {
		t.Fatalf("failed Process() result = %+v, error = %v", result, err)
	}
	var effects int
	if err := db.GetContext(ctx, &effects, "SELECT COUNT(*) FROM inbox_effects WHERE id='effect-retry'"); err != nil {
		t.Fatal(err)
	}
	if effects != 0 {
		t.Fatalf("failed domain effects = %d", effects)
	}

	result, err = store.Process(ctx, key, "integration-test", func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, db.Rebind("INSERT INTO inbox_effects (id,value) VALUES (?,?)"), "effect-retry", "committed")
		return err
	})
	if err != nil || result.Duplicate || result.Attempts != 2 {
		t.Fatalf("retry Process() result = %+v, error = %v", result, err)
	}
	var status string
	var attempts int
	query := db.Rebind("SELECT status,attempts FROM event_inbox WHERE consumer=? AND event_id=?")
	if storeTableIsQualified(db) {
		query = db.Rebind("SELECT status,attempts FROM inbox_test.event_inbox WHERE consumer=? AND event_id=?")
	}
	if err := db.QueryRowxContext(ctx, query, key.Consumer, key.EventID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attempts != 2 {
		t.Fatalf("Inbox status = %q, attempts = %d", status, attempts)
	}
}

func startInboxDatabase(t *testing.T, ctx context.Context, image, exposedPort, driver string, env map[string]string, dsn func(string, string) string) *sqlx.DB {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image, AlwaysPullImage: true, ExposedPorts: []string{exposedPort}, Env: env,
			WaitingFor: wait.ForListeningPort(exposedPort).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, exposedPort)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqlx.Open(driver, dsn(host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	deadline := time.Now().Add(time.Minute)
	for {
		if err = db.PingContext(ctx); err == nil {
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("database did not become ready: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func storeTableIsQualified(db *sqlx.DB) bool { return db.DriverName() == "pgx" }

const postgresInboxSchema = `
CREATE SCHEMA inbox_test;
SET TIME ZONE '+08:00';
CREATE TABLE inbox_test.event_inbox (
  consumer text NOT NULL, event_id text NOT NULL, status text NOT NULL,
  attempts integer NOT NULL, last_error text NOT NULL, completed_at timestamptz,
  version bigint NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
  created_by text NOT NULL, updated_by text NOT NULL,
  PRIMARY KEY (consumer,event_id)
);
CREATE TABLE inbox_effects (id text PRIMARY KEY, value text NOT NULL);`

const mysqlInboxSchema = `
SET time_zone = '+08:00';
CREATE TABLE event_inbox (
  consumer varchar(255) NOT NULL, event_id varchar(255) NOT NULL, status text NOT NULL,
  attempts integer NOT NULL, last_error text NOT NULL, completed_at timestamp(6) NULL,
  version bigint NOT NULL, created_at timestamp(6) NOT NULL, updated_at timestamp(6) NOT NULL,
  created_by text NOT NULL, updated_by text NOT NULL,
  PRIMARY KEY (consumer,event_id)
);
CREATE TABLE inbox_effects (id varchar(255) PRIMARY KEY, value text NOT NULL);`
