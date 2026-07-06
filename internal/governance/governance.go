// Package governance implements GDPR-style customer data export and deletion.
package governance

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/crypto"
	"github.com/dinhphu28/osscdp/internal/profile"
)

// Service performs export and deletion across the customer's data.
type Service struct {
	pool     *pgxpool.Pool
	profiles *profile.Repo
	consent  *consent.Repo
	audit    *audit.Recorder
	cipher   *crypto.Cipher
}

// NewService constructs a Service. cipher (nil-safe) decrypts identity_node
// plaintext for the identifier inventory (Enhancement C Tier 2).
func NewService(pool *pgxpool.Pool, recorder *audit.Recorder, cipher *crypto.Cipher) *Service {
	return &Service{pool: pool, profiles: profile.NewRepo(pool), consent: consent.NewRepo(pool), audit: recorder, cipher: cipher}
}

// IdentityNode is an exported identity node (hashed value only — no raw PII).
type IdentityNode struct {
	Namespace string `json:"namespace"`
	ValueHash string `json:"value_hash"`
}

// Membership is an exported segment membership.
type Membership struct {
	SegmentID string `json:"segment_id"`
	Status    string `json:"status"`
}

// Bundle is the exported customer data.
type Bundle struct {
	Profile       profile.Profile  `json:"profile"`
	IdentityNodes []IdentityNode   `json:"identity_nodes"`
	Memberships   []Membership     `json:"segment_memberships"`
	Consent       []consent.Record `json:"consent"`
}

// Export gathers all data for a customer.
func (s *Service) Export(ctx context.Context, tenantID uuid.UUID, canonicalUserID string) (Bundle, error) {
	p, err := s.profiles.GetByCanonical(ctx, tenantID, canonicalUserID)
	if errors.Is(err, profile.ErrNotFound) {
		return Bundle{}, ErrNotFound
	}
	if err != nil {
		return Bundle{}, err
	}

	b := Bundle{Profile: p}

	nodeRows, err := s.pool.Query(ctx, `
		SELECT n.namespace, n.value_hash
		FROM identity_node n
		JOIN identity_cluster_member m ON m.identity_node_id = n.id
		WHERE m.tenant_id=$1 AND m.cluster_id=$2`, tenantID, p.IdentityClusterID)
	if err != nil {
		return Bundle{}, fmt.Errorf("export nodes: %w", err)
	}
	defer nodeRows.Close()
	b.IdentityNodes = []IdentityNode{}
	for nodeRows.Next() {
		var n IdentityNode
		if err := nodeRows.Scan(&n.Namespace, &n.ValueHash); err != nil {
			return Bundle{}, err
		}
		b.IdentityNodes = append(b.IdentityNodes, n)
	}
	nodeRows.Close()

	memRows, err := s.pool.Query(ctx,
		`SELECT segment_id, status FROM segment_membership WHERE tenant_id=$1 AND customer_profile_id=$2`,
		tenantID, p.ID)
	if err != nil {
		return Bundle{}, fmt.Errorf("export memberships: %w", err)
	}
	defer memRows.Close()
	b.Memberships = []Membership{}
	for memRows.Next() {
		var m Membership
		if err := memRows.Scan(&m.SegmentID, &m.Status); err != nil {
			return Bundle{}, err
		}
		b.Memberships = append(b.Memberships, m)
	}
	memRows.Close()

	b.Consent, err = s.consent.ListForProfile(ctx, tenantID, p.ID)
	if err != nil {
		return Bundle{}, err
	}

	if err := s.audit.Record(ctx, audit.Entry{
		TenantID: &tenantID, ActorType: audit.ActorAdmin, Action: "export",
		ResourceType: "customer_profile", ResourceID: p.ID.String(),
	}); err != nil {
		return Bundle{}, fmt.Errorf("audit export: %w", err)
	}
	return b, nil
}

// IdentifierInventory summarizes all identity nodes linked to a person, grouped
// by namespace. Counts (ByNamespace/Total) answer "how many phones / emails does
// this person have" that the last-write-wins profile traits cannot. Values holds
// the decrypted plaintext per namespace (Tier 2), for nodes ingested after
// encryption was enabled; the handler masks it unless the caller holds pii:read.
type IdentifierInventory struct {
	CanonicalUserID string              `json:"canonical_user_id"`
	Total           int                 `json:"total"`
	ByNamespace     map[string]int      `json:"by_namespace"`
	Values          map[string][]string `json:"values,omitempty"`
}

// Identifiers returns the identifier inventory for a resolved person. It reuses
// the Export cluster-node join and, when a cipher is configured, decrypts each
// node's value_encrypted into Values (nodes lacking ciphertext are counted but
// contribute no value).
func (s *Service) Identifiers(ctx context.Context, tenantID uuid.UUID, canonicalUserID string) (IdentifierInventory, error) {
	p, err := s.profiles.GetByCanonical(ctx, tenantID, canonicalUserID)
	if errors.Is(err, profile.ErrNotFound) {
		return IdentifierInventory{}, ErrNotFound
	}
	if err != nil {
		return IdentifierInventory{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT n.namespace, n.value_encrypted
		FROM identity_node n
		JOIN identity_cluster_member m ON m.identity_node_id = n.id
		WHERE m.tenant_id=$1 AND m.cluster_id=$2
		ORDER BY n.namespace`, tenantID, p.IdentityClusterID)
	if err != nil {
		return IdentifierInventory{}, fmt.Errorf("identifier inventory: %w", err)
	}
	defer rows.Close()
	inv := IdentifierInventory{CanonicalUserID: canonicalUserID, ByNamespace: map[string]int{}}
	for rows.Next() {
		var ns string
		var enc *string
		if err := rows.Scan(&ns, &enc); err != nil {
			return IdentifierInventory{}, err
		}
		inv.ByNamespace[ns]++
		inv.Total++
		if enc == nil || *enc == "" || s.cipher == nil {
			continue
		}
		// Skip (don't fail the request) a node that cannot be decrypted, e.g. a
		// key rotation left old ciphertext undecryptable.
		if v, decErr := s.cipher.Decrypt(*enc); decErr == nil {
			if inv.Values == nil {
				inv.Values = map[string][]string{}
			}
			inv.Values[ns] = append(inv.Values[ns], v)
		}
	}
	return inv, rows.Err()
}

// DeleteCounts reports rows removed per table.
type DeleteCounts struct {
	Profile         int64 `json:"customer_profile"`
	Memberships     int64 `json:"segment_memberships"`
	Consent         int64 `json:"consent"`
	IdentityNodes   int64 `json:"identity_nodes"`
	BehavioralEvent int64 `json:"behavioral_event"`
	BehaviorBucket  int64 `json:"profile_behavior_bucket"`
	PendingEval        int64 `json:"segment_pending_eval"`
	OutboxEmits        int64 `json:"segment_membership_outbox"`
	JourneyEnrollments int64 `json:"journey_enrollment"`
}

// ErrNotFound is returned when the customer profile does not exist.
var ErrNotFound = errors.New("profile not found")

// Delete removes (erases) all customer-scoped data in one transaction. raw_event
// is retained (retention-governed). FK-safe order.
func (s *Service) Delete(ctx context.Context, tenantID uuid.UUID, canonicalUserID string) (DeleteCounts, error) {
	p, err := s.profiles.GetByCanonical(ctx, tenantID, canonicalUserID)
	if errors.Is(err, profile.ErrNotFound) {
		return DeleteCounts{}, ErrNotFound
	}
	if err != nil {
		return DeleteCounts{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeleteCounts{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var counts DeleteCounts

	// Capture identity node ids before deleting membership links.
	var nodeIDs []uuid.UUID
	rows, err := tx.Query(ctx,
		`SELECT identity_node_id FROM identity_cluster_member WHERE tenant_id=$1 AND cluster_id=$2`,
		tenantID, p.IdentityClusterID)
	if err != nil {
		return DeleteCounts{}, fmt.Errorf("collect nodes: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return DeleteCounts{}, err
		}
		nodeIDs = append(nodeIDs, id)
	}
	rows.Close()

	exec := func(sql string, args ...any) (int64, error) {
		ct, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return 0, err
		}
		return ct.RowsAffected(), nil
	}

	if _, err := exec(`DELETE FROM activation_delivery WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if _, err := exec(`DELETE FROM activation_task WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if _, err := exec(`DELETE FROM customer_profile_history WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if counts.Memberships, err = exec(`DELETE FROM segment_membership WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if counts.Consent, err = exec(`DELETE FROM customer_consent WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	// Level-3 behavioral data (behavioral_event.props_json is a second verbatim copy
	// of raw event PII). Erase it before the profile row, same tx (findings #22, #24).
	if counts.PendingEval, err = exec(`DELETE FROM segment_pending_eval WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	// Journey enrollments are profile-keyed with no FK (like segment_pending_eval),
	// so erase them explicitly before the profile row.
	if counts.JourneyEnrollments, err = exec(`DELETE FROM journey_enrollment WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if counts.BehavioralEvent, err = exec(`DELETE FROM behavioral_event WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if counts.BehaviorBucket, err = exec(`DELETE FROM profile_behavior_bucket WHERE tenant_id=$1 AND customer_profile_id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	// The membership-change outbox embeds the profile id (payload) and canonical_user_id
	// (partition_key), so un-drained/published rows would survive erasure. Match on the
	// stable profile id (robust across a canonical change from an earlier merge).
	if counts.OutboxEmits, err = exec(`DELETE FROM segment_membership_outbox WHERE tenant_id=$1 AND payload_json->>'customer_profile_id' = $2`, tenantID, p.ID.String()); err != nil {
		return DeleteCounts{}, err
	}
	if counts.Profile, err = exec(`DELETE FROM customer_profile WHERE tenant_id=$1 AND id=$2`, tenantID, p.ID); err != nil {
		return DeleteCounts{}, err
	}
	if _, err := exec(`DELETE FROM identity_cluster_member WHERE tenant_id=$1 AND cluster_id=$2`, tenantID, p.IdentityClusterID); err != nil {
		return DeleteCounts{}, err
	}
	if len(nodeIDs) > 0 {
		if counts.IdentityNodes, err = exec(`DELETE FROM identity_node WHERE tenant_id=$1 AND id = ANY($2)`, tenantID, nodeIDs); err != nil {
			return DeleteCounts{}, err
		}
	}
	if _, err := exec(`DELETE FROM identity_cluster WHERE tenant_id=$1 AND id=$2`, tenantID, p.IdentityClusterID); err != nil {
		return DeleteCounts{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return DeleteCounts{}, fmt.Errorf("commit delete: %w", err)
	}

	if err := s.audit.Record(ctx, audit.Entry{
		TenantID: &tenantID, ActorType: audit.ActorAdmin, Action: "delete",
		ResourceType: "customer_profile", ResourceID: p.ID.String(), After: counts,
	}); err != nil {
		return DeleteCounts{}, fmt.Errorf("audit delete: %w", err)
	}
	return counts, nil
}
