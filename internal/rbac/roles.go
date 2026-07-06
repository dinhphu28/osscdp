// Package rbac defines roles, permissions, admin tokens, and PII masking.
// See docs/cdp/08-governance-security-observability.md.
package rbac

import "github.com/google/uuid"

// Roles.
const (
	RoleSuperAdmin  = "SUPER_ADMIN"
	RoleTenantAdmin = "TENANT_ADMIN"
	RoleMarketer    = "MARKETER"
	RoleAnalyst     = "ANALYST"
	RoleOperator    = "OPERATOR"
	RoleViewer      = "VIEWER"
)

// Permissions.
const (
	PermSourceRead       = "source:read"
	PermSourceWrite      = "source:write"
	PermEventRead        = "event:read"
	PermEventReplay      = "event:replay"
	PermProfileRead      = "profile:read"
	PermProfileDelete    = "profile:delete"
	PermSegmentRead      = "segment:read"
	PermSegmentWrite     = "segment:write"
	PermJourneyRead      = "journey:read"
	PermJourneyWrite     = "journey:write"
	PermDestinationRead  = "destination:read"
	PermDestinationWrite = "destination:write"
	PermActivationRead   = "activation:read"
	PermDLQRead          = "dlq:read"
	PermDLQRetry         = "dlq:retry"
	PermAuditRead        = "audit:read"
	PermConsentWrite     = "consent:write"
	PermPIIRead          = "pii:read"
	PermAdminWrite       = "admin:write"
)

// allPerms is the full permission set (SUPER_ADMIN / TENANT_ADMIN).
var allPerms = perms(
	PermSourceRead, PermSourceWrite, PermEventRead, PermEventReplay,
	PermProfileRead, PermProfileDelete, PermSegmentRead, PermSegmentWrite,
	PermJourneyRead, PermJourneyWrite,
	PermDestinationRead, PermDestinationWrite, PermActivationRead,
	PermDLQRead, PermDLQRetry, PermAuditRead, PermConsentWrite, PermPIIRead, PermAdminWrite,
)

var readPerms = perms(
	PermSourceRead, PermEventRead, PermProfileRead, PermSegmentRead, PermJourneyRead,
	PermDestinationRead, PermActivationRead, PermAuditRead, PermDLQRead,
)

// rolePerms maps each role to its permission set.
var rolePerms = map[string]map[string]bool{
	RoleSuperAdmin:  allPerms,
	RoleTenantAdmin: allPerms,
	RoleMarketer: union(readPerms, perms(
		PermSegmentWrite, PermJourneyWrite, PermDestinationWrite, PermConsentWrite,
	)),
	RoleAnalyst:  readPerms,
	RoleOperator: union(readPerms, perms(PermDLQRetry, PermEventReplay)),
	RoleViewer:   readPerms,
}

// ValidRole reports whether r is a known role.
func ValidRole(r string) bool { _, ok := rolePerms[r]; return ok }

// Has reports whether the role grants the permission.
func Has(role, perm string) bool { return rolePerms[role][perm] }

// Principal is an authenticated admin caller.
type Principal struct {
	Role     string
	TenantID *uuid.UUID // nil = cross-tenant (super-admin)
}

// IsSuperAdmin reports cross-tenant scope.
func (p Principal) IsSuperAdmin() bool { return p.Role == RoleSuperAdmin || p.TenantID == nil }

// Can reports whether the principal holds the permission.
func (p Principal) Can(perm string) bool { return Has(p.Role, perm) }

func perms(list ...string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, p := range list {
		m[p] = true
	}
	return m
}

func union(a, b map[string]bool) map[string]bool {
	out := make(map[string]bool, len(a)+len(b))
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}
