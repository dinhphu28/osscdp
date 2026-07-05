package behavior

import "testing"

func TestPropsShapeVersion(t *testing.T) {
	// Same top-level shape, different key order → same fingerprint.
	if a, b := propsShapeVersion([]byte(`{"a":1,"b":"x"}`)), propsShapeVersion([]byte(`{"b":"y","a":2}`)); a != b {
		t.Fatalf("key order / value change within same shape must match: %d != %d", a, b)
	}
	// A key changing JSON type → different fingerprint.
	if a, b := propsShapeVersion([]byte(`{"a":1}`)), propsShapeVersion([]byte(`{"a":"x"}`)); a == b {
		t.Fatalf("number->string drift must differ: both %d", a)
	}
	if a, b := propsShapeVersion([]byte(`{"a":1}`)), propsShapeVersion([]byte(`{"a":{"v":1}}`)); a == b {
		t.Fatalf("number->object drift must differ: both %d", a)
	}
	// Empty / absent / non-object props all map to the DEFAULT of 1.
	for _, raw := range []string{``, `null`, `[]`, `[1,2]`, `"scalar"`, `123`, `{}`, `   `} {
		if v := propsShapeVersion([]byte(raw)); v != 1 {
			t.Fatalf("shape-neutral input %q should map to 1, got %d", raw, v)
		}
	}
	// A populated object fits a positive int32.
	if v := propsShapeVersion([]byte(`{"price":100,"currency":"USD"}`)); v <= 0 {
		t.Fatalf("fingerprint must be a positive int32, got %d", v)
	}
}
