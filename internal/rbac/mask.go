package rbac

import "strings"

// MaskEmail masks the local part: user@example.com → u***@example.com.
func MaskEmail(email string) string {
	at := strings.IndexByte(email, '@')
	if at <= 0 {
		return MaskName(email)
	}
	local, domain := email[:at], email[at:]
	if len(local) == 0 {
		return "***" + domain
	}
	return local[:1] + "***" + domain
}

// MaskPhone keeps the first 5 and last 3 characters: +84901234567 → +8490****567.
func MaskPhone(phone string) string {
	if len(phone) <= 8 {
		if phone == "" {
			return ""
		}
		return "****"
	}
	return phone[:5] + "****" + phone[len(phone)-3:]
}

// MaskName keeps the first character: Nguyen → N***.
func MaskName(name string) string {
	if name == "" {
		return ""
	}
	return name[:1] + "***"
}

// MaskTraits returns a copy of traits with email/phone/name masked.
func MaskTraits(traits map[string]any) map[string]any {
	if traits == nil {
		return nil
	}
	out := make(map[string]any, len(traits))
	for k, v := range traits {
		out[k] = v
	}
	if s, ok := out["email"].(string); ok {
		out["email"] = MaskEmail(s)
	}
	if s, ok := out["phone"].(string); ok {
		out["phone"] = MaskPhone(s)
	}
	if s, ok := out["name"].(string); ok {
		out["name"] = MaskName(s)
	}
	return out
}
