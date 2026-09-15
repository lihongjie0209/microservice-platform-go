// Package redact removes secrets from structured payloads before they cross
// trust boundaries such as audit storage and application logs.
package redact

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode"
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

// ContainsSensitiveJSON reports whether a JSON object contains a credential-
// bearing key at any nesting depth. Invalid JSON is returned as an error.
func ContainsSensitiveJSON(data []byte) (bool, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return false, nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return false, err
	}
	return containsSensitiveValue(value), nil
}

// SensitiveKey recognizes credential-bearing structured field names without
// treating unrelated substrings such as "monkey" or "tokenized" as secrets.
func SensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if _, sensitive := defaultSensitiveKeys[normalized]; sensitive {
		return true
	}
	parts := strings.FieldsFunc(normalized, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	for _, part := range parts {
		switch part {
		case "password", "passwd", "secret", "token", "authorization", "cookie", "credential", "credentials", "psk":
			return true
		}
	}
	for index := 0; index+1 < len(parts); index++ {
		if parts[index+1] == "key" && (parts[index] == "api" || parts[index] == "access" || parts[index] == "private") {
			return true
		}
	}
	return false
}

func redactValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if SensitiveKey(key) {
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

func containsSensitiveValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if SensitiveKey(key) || containsSensitiveValue(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsSensitiveValue(child) {
				return true
			}
		}
	}
	return false
}
