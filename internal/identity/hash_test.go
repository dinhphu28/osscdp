package identity

import (
	"testing"

	"github.com/google/uuid"
)

func TestValueHash_Deterministic(t *testing.T) {
	tid := uuid.New()
	if ValueHash(tid, NSEmail, "u@x.com") != ValueHash(tid, NSEmail, "u@x.com") {
		t.Fatal("ValueHash must be deterministic")
	}
}

func TestValueHash_TenantScoped(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	if ValueHash(a, NSEmail, "u@x.com") == ValueHash(b, NSEmail, "u@x.com") {
		t.Fatal("same value in different tenants must hash differently")
	}
}

func TestValueHash_NamespaceScoped(t *testing.T) {
	tid := uuid.New()
	if ValueHash(tid, NSUserID, "v1") == ValueHash(tid, NSAnonymousID, "v1") {
		t.Fatal("same value in different namespaces must hash differently")
	}
}
