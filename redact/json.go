// Package redact removes secrets from structured payloads before they cross
// trust boundaries such as audit storage and application logs.
package redact

import (
	"bytes"
	"encoding/json"
	"strings"
)

var defaultSensitiveKeys = map[string]struct{}{
	"authorization": {}, "client_secret": {}, "password": {}, "refresh_token": {},
	"secret": {}, "token": {}, "access_token": {}, "api_key": {}, "psk": {},
}

// JSON redacts known sensitive keys recursively. Invalid JSON is rejected so
// callers never persist an opaque payload under a false safety assumption.
func JSON(data []byte) ([]byte, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	redactValue(value)
	return json.Marshal(value)
}

func redactValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if _, sensitive := defaultSensitiveKeys[strings.ToLower(strings.TrimSpace(key))]; sensitive {
				typed[key] = "[REDACTED]"
				continue
			}
			redactValue(child)
		}
	case []any:
		for _, child := range typed {
			redactValue(child)
		}
	}
}
