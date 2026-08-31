package importprovider

import (
	"reflect"
	"testing"
)

func TestMetadataRoundTripIsCanonical(t *testing.T) {
	metadata, err := Metadata([]Dataset{{Code: "identity.users", Title: "Users", Formats: []string{"xlsx", "csv", "csv"}, MaxBatchSize: 500, SupportsDryRun: true}})
	if err != nil {
		t.Fatal(err)
	}
	values, err := ParseMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || !reflect.DeepEqual(values[0].Formats, []string{"csv", "xlsx"}) || values[0].MaxBatchSize != 500 || !values[0].SupportsDryRun {
		t.Fatalf("datasets=%+v", values)
	}
}

func TestMetadataRejectsInvalidDefinitions(t *testing.T) {
	tests := [][]Dataset{nil, {{Code: "Invalid Code", Title: "x", Formats: []string{"csv"}, MaxBatchSize: 1}}, {{Code: "users", Title: "", Formats: []string{"csv"}, MaxBatchSize: 1}}, {{Code: "users", Title: "Users", Formats: []string{"pdf"}, MaxBatchSize: 1}}, {{Code: "users", Title: "Users", Formats: []string{"csv"}, MaxBatchSize: 0}}, {{Code: "users", Title: "Users", Formats: []string{"csv"}, MaxBatchSize: 1}, {Code: "users", Title: "Again", Formats: []string{"csv"}, MaxBatchSize: 1}}}
	for _, values := range tests {
		if _, err := Metadata(values); err == nil {
			t.Fatalf("accepted %+v", values)
		}
	}
}
