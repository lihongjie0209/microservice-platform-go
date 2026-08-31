// Package exportprovider defines the service-registry metadata contract used
// by services that implement platform.export.v1.ExportProviderService.
package exportprovider

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const (
	ProviderMetadataKey = "platform.export.provider"
	DatasetsMetadataKey = "platform.export.datasets"
)

var datasetCode = regexp.MustCompile(`^[a-z][a-z0-9._-]{1,127}$`)
var supportedFormats = []string{"csv", "jsonl", "xlsx"}

type Dataset struct {
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Formats          []string `json:"formats"`
	SupportsSnapshot bool     `json:"supports_snapshot"`
}

func Metadata(datasets []Dataset) (map[string]string, error) {
	normalized, err := Normalize(datasets)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("encode export datasets: %w", err)
	}
	return map[string]string{ProviderMetadataKey: "true", DatasetsMetadataKey: string(payload)}, nil
}

func Normalize(datasets []Dataset) ([]Dataset, error) {
	if len(datasets) == 0 {
		return nil, errors.New("at least one export dataset is required")
	}
	result := make([]Dataset, 0, len(datasets))
	seen := map[string]struct{}{}
	for _, value := range datasets {
		value.Code = strings.ToLower(strings.TrimSpace(value.Code))
		value.Title = strings.TrimSpace(value.Title)
		if !datasetCode.MatchString(value.Code) || value.Title == "" {
			return nil, fmt.Errorf("dataset %q requires a valid code and title", value.Code)
		}
		if _, exists := seen[value.Code]; exists {
			return nil, fmt.Errorf("duplicate export dataset %q", value.Code)
		}
		seen[value.Code] = struct{}{}
		formats := make([]string, 0, len(value.Formats))
		for _, format := range value.Formats {
			format = strings.ToLower(strings.TrimSpace(format))
			if !slices.Contains(supportedFormats, format) {
				return nil, fmt.Errorf("dataset %q has unsupported format %q", value.Code, format)
			}
			if !slices.Contains(formats, format) {
				formats = append(formats, format)
			}
		}
		if len(formats) == 0 {
			return nil, fmt.Errorf("dataset %q requires at least one format", value.Code)
		}
		slices.Sort(formats)
		value.Formats = formats
		result = append(result, value)
	}
	slices.SortFunc(result, func(a, b Dataset) int { return strings.Compare(a.Code, b.Code) })
	return result, nil
}

func ParseMetadata(metadata map[string]string) ([]Dataset, error) {
	if metadata[ProviderMetadataKey] != "true" {
		return nil, errors.New("instance is not an export provider")
	}
	var datasets []Dataset
	if err := json.Unmarshal([]byte(metadata[DatasetsMetadataKey]), &datasets); err != nil {
		return nil, fmt.Errorf("decode export datasets: %w", err)
	}
	return Normalize(datasets)
}
