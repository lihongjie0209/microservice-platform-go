package errorcode_test

import (
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/errorcode"
)

func TestCode_Valid(t *testing.T) {
	if !errorcode.StaleVersion.Valid() {
		t.Fatal("StaleVersion.Valid() = false")
	}
	if errorcode.Code(99999).Valid() {
		t.Fatal("unknown code.Valid() = true")
	}
}
