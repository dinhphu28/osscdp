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
	// The journey admin endpoints must be documented.
	if doc.Paths.Find("/admin/v1/tenants/{tenantID}/journeys") == nil {
		t.Fatal("spec is missing the journeys endpoints")
	}
	if _, ok := doc.Components.Schemas["Journey"]; !ok {
		t.Fatal("spec is missing the Journey schema")
	}
}
