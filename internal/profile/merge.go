package profile

import (
	"encoding/json"
	"time"

	"github.com/dinhphu28/osscdp/internal/events"
)

// MergeTraits applies the trait merge policy (latest non-empty wins) from an
// event onto the current traits, returning the new map and the changed keys.
func MergeTraits(cur map[string]any, env events.Envelope) (map[string]any, []string) {
	out := cloneMap(cur)
	var changed []string
	set := func(key, val string) {
		if val == "" {
			return
		}
		if s, _ := out[key].(string); s != val {
			out[key] = val
			changed = append(changed, "traits."+key)
		}
	}

	tr := parseObject(env.Traits)
	set(TraitEmail, asString(tr[TraitEmail]))
	set(TraitPhone, asString(tr[TraitPhone]))
	set(TraitName, asString(tr[TraitName]))
	set(TraitCountry, asString(tr[TraitCountry]))
	// Identifiers may also carry email/phone (already normalized); they win when present.
	set(TraitEmail, env.Identifiers.Email)
	set(TraitPhone, env.Identifiers.Phone)
	return out, changed
}

// MergeComputed applies computed-attribute updates for one event.
func MergeComputed(cur map[string]any, env events.Envelope) (map[string]any, []string) {
	out := cloneMap(cur)
	var changed []string

	out[AttrTotalEvents] = asInt(out[AttrTotalEvents]) + 1
	changed = append(changed, "computed_attributes."+AttrTotalEvents)

	setStr := func(key, val string) {
		if val == "" {
			return
		}
		if s, _ := out[key].(string); s != val {
			out[key] = val
			changed = append(changed, "computed_attributes."+key)
		}
	}

	setStr(AttrLastEventName, env.EventName)
	setStr(AttrLastSourceID, env.SourceID.String())
	setStr(AttrLastPageURL, pageURL(env.Context))

	if env.EventName == EventProductViewed {
		setStr(AttrLastProductViewed, propString(env.Properties, "product_id"))
	}
	if env.EventName == EventOrderCompleted {
		out[AttrTotalOrders] = asInt(out[AttrTotalOrders]) + 1
		out[AttrLastOrderAt] = env.Timestamp.UTC().Format(time.RFC3339)
		changed = append(changed, "computed_attributes."+AttrTotalOrders, "computed_attributes."+AttrLastOrderAt)
	}
	return out, changed
}

// mergeReparent folds a loser profile into the survivor when two identity
// clusters merge. Policy (non-destructive to the survivor): traits and computed
// attributes fill only keys the survivor is missing; event/order counts are
// summed; first/last seen are widened. Returns the changed field names.
func mergeReparent(dst *Profile, src Profile) []string {
	var changed []string

	for k, v := range src.Traits {
		if _, ok := dst.Traits[k]; !ok {
			dst.Traits[k] = v
			changed = append(changed, "traits."+k)
		}
	}

	// Sum the running counts across both profiles.
	for _, key := range []string{AttrTotalEvents, AttrTotalOrders} {
		if n := asInt(src.ComputedAttributes[key]); n > 0 {
			dst.ComputedAttributes[key] = asInt(dst.ComputedAttributes[key]) + n
			changed = append(changed, "computed_attributes."+key)
		}
	}
	// The "last_*" activity block reflects the most recent event: take it from
	// whichever profile was seen more recently, not fill-missing (which would keep
	// a staler survivor value). Fill when the survivor lacks the key.
	loserNewer := src.LastSeenAt != nil && (dst.LastSeenAt == nil || src.LastSeenAt.After(*dst.LastSeenAt))
	for _, key := range []string{AttrLastEventName, AttrLastSourceID, AttrLastPageURL, AttrLastProductViewed} {
		v, ok := src.ComputedAttributes[key]
		if !ok {
			continue
		}
		if _, has := dst.ComputedAttributes[key]; (!has || loserNewer) && dst.ComputedAttributes[key] != v {
			dst.ComputedAttributes[key] = v
			changed = append(changed, "computed_attributes."+key)
		}
	}
	// last_order_at is a plain timestamp: keep the later of the two (RFC3339 sorts lexically).
	if v := asString(src.ComputedAttributes[AttrLastOrderAt]); v != "" && v > asString(dst.ComputedAttributes[AttrLastOrderAt]) {
		dst.ComputedAttributes[AttrLastOrderAt] = v
		changed = append(changed, "computed_attributes."+AttrLastOrderAt)
	}
	// Any other computed key: fill only what the survivor lacks.
	for k, v := range src.ComputedAttributes {
		switch k {
		case AttrTotalEvents, AttrTotalOrders, AttrLastEventName, AttrLastSourceID, AttrLastPageURL, AttrLastProductViewed, AttrLastOrderAt:
			continue
		}
		if _, ok := dst.ComputedAttributes[k]; !ok {
			dst.ComputedAttributes[k] = v
			changed = append(changed, "computed_attributes."+k)
		}
	}

	if src.FirstSeenAt != nil && (dst.FirstSeenAt == nil || src.FirstSeenAt.Before(*dst.FirstSeenAt)) {
		dst.FirstSeenAt = src.FirstSeenAt
		changed = append(changed, "first_seen_at")
	}
	if src.LastSeenAt != nil && (dst.LastSeenAt == nil || src.LastSeenAt.After(*dst.LastSeenAt)) {
		dst.LastSeenAt = src.LastSeenAt
		changed = append(changed, "last_seen_at")
	}
	return changed
}

// MergeSeen returns the earliest first_seen and latest last_seen given the
// event timestamp, plus whether either changed.
func MergeSeen(first, last *time.Time, ts time.Time) (newFirst, newLast *time.Time, changed []string) {
	ts = ts.UTC()
	newFirst, newLast = first, last
	if first == nil || ts.Before(*first) {
		t := ts
		newFirst = &t
		changed = append(changed, "first_seen_at")
	}
	if last == nil || ts.After(*last) {
		t := ts
		newLast = &t
		changed = append(changed, "last_seen_at")
	}
	return newFirst, newLast, changed
}

// --- helpers ---

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+4)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// asInt coerces a JSON-decoded number (float64) or native int/int64 to int64.
func asInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func parseObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func propString(raw json.RawMessage, key string) string {
	return asString(parseObject(raw)[key])
}

// pageURL extracts context.page.url, best-effort.
func pageURL(raw json.RawMessage) string {
	page, _ := parseObject(raw)["page"].(map[string]any)
	return asString(page["url"])
}
