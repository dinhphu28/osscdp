package foundationtest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/crypto"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/governance"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/rawevent"
	"github.com/dinhphu28/osscdp/internal/source"
)

func TestSource_RotateKey(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tn, err := f.tenantSvc.Create(ctx, "acme")
	require.NoError(t, err)
	created, err := f.sourceSvc.Create(ctx, tn.ID, "web", "server")
	require.NoError(t, err)
	oldKey := created.APIKey

	newKey, err := f.sourceSvc.RotateKey(ctx, tn.ID, created.Source.ID)
	require.NoError(t, err)
	require.NotEqual(t, oldKey, newKey)

	// Old key no longer authenticates; new key does.
	_, err = f.sourceSvc.Authenticate(ctx, oldKey)
	require.ErrorIs(t, err, source.ErrNotFound)
	got, err := f.sourceSvc.Authenticate(ctx, newKey)
	require.NoError(t, err)
	require.Equal(t, created.Source.ID, got.ID)

	// Rotation audited.
	var n int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND action='rotate_key'`, tn.ID).Scan(&n))
	require.Equal(t, 1, n)
}

func TestConsentGate_DeniedSkips(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)

	arepo := activation.NewRepo(f.pool)
	// webhook destination with channel/purpose in config.
	cfg, _ := json.Marshal(map[string]any{"url": "http://127.0.0.1:9", "channel": "webhook", "purpose": "marketing"})
	dest, err := arepo.CreateDestination(ctx, tid, activation.TypeWebhook, "wh", cfg, "")
	require.NoError(t, err)
	segID := uuid.New()
	_, err = arepo.CreateSubscription(ctx, tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)

	crepo := consent.NewRepo(f.pool)
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool), crepo)

	// Denied → skipped task.
	require.NoError(t, crepo.Set(ctx, tid, pu.CustomerProfileID, "webhook", "marketing", consent.StatusDenied, "test"))
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pu.CustomerProfileID, "entered")))

	var status string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM activation_task WHERE tenant_id=$1`, tid).Scan(&status))
	require.Equal(t, activation.TaskSkipped, status)
}

func TestConsentGate_GrantedProceeds(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)

	arepo := activation.NewRepo(f.pool)
	cfg, _ := json.Marshal(map[string]any{"url": "http://127.0.0.1:9", "channel": "webhook", "purpose": "marketing"})
	dest, err := arepo.CreateDestination(ctx, tid, activation.TypeWebhook, "wh", cfg, "")
	require.NoError(t, err)
	segID := uuid.New()
	_, err = arepo.CreateSubscription(ctx, tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)

	crepo := consent.NewRepo(f.pool)
	require.NoError(t, crepo.Set(ctx, tid, pu.CustomerProfileID, "webhook", "marketing", consent.StatusGranted, "test"))
	svc := activation.NewService(f.pool, profile.NewRepo(f.pool), crepo)
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pu.CustomerProfileID, "entered")))

	var status string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM activation_task WHERE tenant_id=$1`, tid).Scan(&status))
	require.Equal(t, activation.TaskPending, status)
}

func TestGovernance_ExportThenDelete(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"email":"u@x.com","country":"VN"}`)
	require.NoError(t, consent.NewRepo(f.pool).Set(ctx, tid, pu.CustomerProfileID, "webhook", "marketing", consent.StatusGranted, "t"))
	// Seed a raw_event so the "retained after delete" assertion is meaningful.
	rawPayload, err := pu.Event.PayloadJSON()
	require.NoError(t, err)
	require.NoError(t, rawevent.NewRepo(f.pool).Store(ctx, pu.Event, rawPayload))

	gov := governance.NewService(f.pool, audit.NewRecorder(f.pool), nil)

	bundle, err := gov.Export(ctx, tid, pu.CanonicalUserID)
	require.NoError(t, err)
	require.Equal(t, pu.CanonicalUserID, bundle.Profile.CanonicalUserID)
	require.NotEmpty(t, bundle.IdentityNodes)
	require.Len(t, bundle.Consent, 1)

	counts, err := gov.Delete(ctx, tid, pu.CanonicalUserID)
	require.NoError(t, err)
	require.Equal(t, int64(1), counts.Profile)

	// Profile gone; raw_event retained.
	_, err = profile.NewRepo(f.pool).GetByCanonical(ctx, tid, pu.CanonicalUserID)
	require.ErrorIs(t, err, profile.ErrNotFound)
	var raw int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM raw_event WHERE tenant_id=$1`, tid).Scan(&raw))
	require.Greater(t, raw, 0)

	// Audited.
	var n int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND action IN ('export','delete')`, tid).Scan(&n))
	require.Equal(t, 2, n)
}

func TestWebhookSigning_HeaderPresent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	seedProfile(t, f, tid, sid, "ev", "product_viewed", "u1", `{"country":"VN"}`)
	pid := profileIDFor(t, f, tid)

	var gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-CDP-Signature")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	key := mustKey(t)
	cipher, err := crypto.New(key)
	require.NoError(t, err)
	secret, err := cipher.Encrypt("webhook-secret")
	require.NoError(t, err)

	arepo := activation.NewRepo(f.pool)
	cfg, _ := json.Marshal(activation.WebhookConfig{URL: srv.URL, TimeoutMS: 2000})
	dest, err := arepo.CreateDestination(ctx, tid, activation.TypeWebhook, "wh", cfg, secret)
	require.NoError(t, err)
	segID := uuid.New()
	_, err = arepo.CreateSubscription(ctx, tid, dest.ID, activation.TriggerSegmentMembership, &segID)
	require.NoError(t, err)

	svc := activation.NewService(f.pool, profile.NewRepo(f.pool), nil)
	require.NoError(t, svc.OnMembershipChanged(ctx, mc(tid, segID, pid, "entered")))

	runner := activation.NewRunner(f.pool, map[string]activation.Sender{
		activation.TypeWebhook: activation.NewWebhookSender(cipher),
	}, 50, time.Second, testLogger())
	_, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Contains(t, gotSig, "sha256=")
}

// TestIdentifiers_Tier2Values exercises Enhancement C Tier 2 end-to-end: identity
// encrypts each identifier at ingest, and the governance inventory decrypts them.
func TestIdentifiers_Tier2Values(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	cipher, err := crypto.New(mustKey(t))
	require.NoError(t, err)

	idSvc := identity.NewService(f.pool, noopPub{}, bus.TopicIdentityResolved)
	idSvc.Cipher = cipher
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)

	// Ingest an event carrying user_id + email + phone -> three encrypted nodes.
	env := mergeEnv(t, tid, sid, "t2", events.Identifiers{UserID: "u1", Email: "e@x.com", Phone: "+8490"}, "", time.Now())
	canonical, _ := resolveAndProfile(t, f, idSvc, profSvc, env)

	// With a cipher, the inventory returns the decrypted plaintext values.
	gov := governance.NewService(f.pool, audit.NewRecorder(f.pool), cipher)
	inv, err := gov.Identifiers(ctx, tid, canonical)
	require.NoError(t, err)
	require.Equal(t, 3, inv.Total)
	require.Equal(t, []string{"e@x.com"}, inv.Values["email"])
	require.Equal(t, []string{"+8490"}, inv.Values["phone"])
	require.Equal(t, []string{"u1"}, inv.Values["user_id"])

	// Without a cipher, values are omitted; counts still returned.
	govNoCipher := governance.NewService(f.pool, audit.NewRecorder(f.pool), nil)
	inv2, err := govNoCipher.Identifiers(ctx, tid, canonical)
	require.NoError(t, err)
	require.Equal(t, 3, inv2.Total)
	require.Nil(t, inv2.Values)
}

func mustKey(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i)
	}
	return base64.StdEncoding.EncodeToString(b)
}
