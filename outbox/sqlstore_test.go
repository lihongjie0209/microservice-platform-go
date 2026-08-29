package outbox

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
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
