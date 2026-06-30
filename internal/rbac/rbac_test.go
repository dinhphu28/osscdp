package rbac

import "testing"

func TestHas_RoleMatrix(t *testing.T) {
	cases := []struct {
		role string
		perm string
		want bool
	}{
		{RoleSuperAdmin, PermAdminWrite, true},
		{RoleSuperAdmin, PermPIIRead, true},
		{RoleTenantAdmin, PermProfileDelete, true},
		{RoleTenantAdmin, PermPIIRead, true},
		{RoleViewer, PermSegmentRead, true},
		{RoleViewer, PermSegmentWrite, false},
		{RoleViewer, PermPIIRead, false},
		{RoleAnalyst, PermProfileRead, true},
		{RoleAnalyst, PermProfileDelete, false},
		{RoleMarketer, PermSegmentWrite, true},
		{RoleMarketer, PermPIIRead, false},
		{RoleOperator, PermEventReplay, true},
		{RoleOperator, PermSegmentWrite, false},
	}
	for _, c := range cases {
		if got := Has(c.role, c.perm); got != c.want {
			t.Errorf("Has(%s, %s) = %v, want %v", c.role, c.perm, got, c.want)
		}
	}
}

func TestValidRole(t *testing.T) {
	if !ValidRole(RoleViewer) || ValidRole("BOGUS") {
		t.Fatal("ValidRole wrong")
	}
}

func TestMask(t *testing.T) {
	if got := MaskEmail("user@example.com"); got != "u***@example.com" {
		t.Fatalf("MaskEmail = %q", got)
	}
	if got := MaskPhone("+84901234567"); got != "+8490****567" {
		t.Fatalf("MaskPhone = %q", got)
	}
	if got := MaskName("Nguyen"); got != "N***" {
		t.Fatalf("MaskName = %q", got)
	}
	if MaskEmail("") != "" || MaskPhone("") != "" || MaskName("") != "" {
		t.Fatal("empty inputs should stay empty")
	}
}

func TestMaskTraits(t *testing.T) {
	in := map[string]any{"email": "u@x.com", "phone": "+84901234567", "name": "Ann", "country": "VN"}
	out := MaskTraits(in)
	if out["email"] == "u@x.com" || out["country"] != "VN" {
		t.Fatalf("MaskTraits wrong: %+v", out)
	}
	// Original unchanged.
	if in["email"] != "u@x.com" {
		t.Fatal("MaskTraits must not mutate the input")
	}
}

func TestGenerateToken(t *testing.T) {
	pt, hash, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	if !LooksLikeToken(pt) || pt == hash || HashToken(pt) != hash {
		t.Fatal("token generation/hash inconsistent")
	}
}
