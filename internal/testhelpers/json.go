package testhelpers

import (
	"encoding/json"
	"testing"
)

// NormalizeJSON normalizes the input json string to look like how it would look
// as if marshaled from a json.Marshal. In particular, this may reorder some
// fields (e.g. json object keys are sorted alphabetically), but the json should
// be equivalent.
func NormalizeJSON(t *testing.T, jsonIn []byte) []byte {
	t.Helper()
	var tmp interface{}
	err := json.Unmarshal(jsonIn, &tmp)
	if err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(tmp)
	if err != nil {
		t.Fatal(err)
	}
	return output
}
