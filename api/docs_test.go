package apidocs

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestOpenAPISpecIsValid(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(Spec())
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := doc.Validate(loader.Context); err != nil {
		t.Fatalf("validate spec: %v", err)
	}
	if len(doc.Paths.Map()) == 0 {
		t.Fatal("spec has no paths")
	}
}
