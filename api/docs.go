// Package apidocs embeds the OpenAPI spec and serves it plus a Redoc viewer.
package apidocs

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var spec []byte

// Spec returns the raw OpenAPI YAML.
func Spec() []byte { return spec }

// SpecHandler serves the OpenAPI spec at e.g. /openapi.yaml.
func SpecHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(spec)
	}
}

const redocHTML = `<!DOCTYPE html>
<html>
  <head>
    <title>osscdp API</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <style>body { margin: 0; padding: 0; }</style>
  </head>
  <body>
    <redoc spec-url="/openapi.yaml"></redoc>
    <script src="https://cdn.redoc.ly/redoc/v2.1.5/bundles/redoc.standalone.js"
            integrity="sha384-0GrsyTQc9Oqd8h+b2dbc4XdR2T/DYpy0tLNNstyx+LBMUyiBbcWPbEs9aRmUcaxD"
            crossorigin="anonymous"></script>
  </body>
</html>`

// DocsHandler serves a Redoc viewer that loads /openapi.yaml.
func DocsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(redocHTML))
	}
}
