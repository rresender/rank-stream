package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func f(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func r(r *mux.Route) {
	r.Path("/test").Methods(http.MethodGet)
}

func TestHandler(t *testing.T) {
	h := &Handler{
		Route: r,
		Func:  f,
	}
	r := mux.NewRouter()
	h.AddRoute(r)
	err := r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err := route.GetPathTemplate()
		if err == nil {
			assert.Equal(t, "/test", pathTemplate)
		}
		methods, err := route.GetMethods()
		if err == nil {
			assert.Equal(t, http.MethodGet, strings.Join(methods, ","))
		}
		return nil
	})
	assert.Nil(t, err)
	assert.NotNil(t, h.Route)
	assert.NotNil(t, h.Func)
}
