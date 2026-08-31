package exportprovider

import (
	"reflect"
	"testing"
)

func TestMetadataRoundTripIsCanonical(t *testing.T) {
	metadata, err := Metadata([]Dataset{{Code: "billing.invoices", Title: "Invoices", Formats: []string{"xlsx", "csv", "csv"}, SupportsSnapshot: true}})
	if err != nil {
		t.Fatal(err)
	}
	values, err := ParseMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"csv", "xlsx"}
	if len(values) != 1 || !reflect.DeepEqual(values[0].Formats, want) || !values[0].SupportsSnapshot {
		t.Fatalf("datasets=%+v", values)
	}
}
func TestMetadataRejectsInvalidDefinitions(t *testing.T) {
	tests := [][]Dataset{nil, {{Code: "Invalid Code", Title: "x", Formats: []string{"csv"}}}, {{Code: "users", Title: "", Formats: []string{"csv"}}}, {{Code: "users", Title: "Users", Formats: []string{"pdf"}}}, {{Code: "users", Title: "Users", Formats: []string{"csv"}}, {Code: "users", Title: "Again", Formats: []string{"csv"}}}}
	for _, values := range tests {
		if _, err := Metadata(values); err == nil {
			t.Fatalf("accepted %+v", values)
		}
	}
}
