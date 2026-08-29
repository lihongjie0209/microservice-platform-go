package redact

import (
	"strings"
	"testing"
)

func TestJSONRedactsSensitiveKeysRecursively(t *testing.T) {
	result, err := JSON([]byte(`{"user":"alice","password":"pw","nested":{"Authorization":"Bearer x"},"items":[{"token":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	text := string(result)
	if strings.Contains(text, "pw") || strings.Contains(text, "Bearer x") || strings.Contains(text, `"x"`) {
		t.Fatalf("secret remained in %s", text)
	}
	if strings.Count(text, "[REDACTED]") != 3 {
		t.Fatalf("unexpected redaction result %s", text)
	}
}

func TestJSONRejectsInvalidPayload(t *testing.T) {
	if _, err := JSON([]byte(`{"password":`)); err == nil {
		t.Fatal("JSON() accepted invalid JSON")
	}
}
