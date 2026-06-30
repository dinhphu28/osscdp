// Package apierror defines the shared JSON error envelope used across the CDP HTTP APIs.
package apierror

import (
	"encoding/json"
	"net/http"
)

// Error is the canonical error body returned by every API endpoint.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type envelope struct {
	Error Error `json:"error"`
}

// Write serializes a structured error to the response.
func Write(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope{Error: Error{Code: code, Message: message}})
}

// Common helpers.
func BadRequest(w http.ResponseWriter, message string)   { Write(w, http.StatusBadRequest, "bad_request", message) }
func Unauthorized(w http.ResponseWriter, message string) { Write(w, http.StatusUnauthorized, "unauthorized", message) }
func Conflict(w http.ResponseWriter, message string)     { Write(w, http.StatusConflict, "conflict", message) }
func Internal(w http.ResponseWriter)                      { Write(w, http.StatusInternalServerError, "internal_error", "internal server error") }
func NotFound(w http.ResponseWriter, message string)     { Write(w, http.StatusNotFound, "not_found", message) }
