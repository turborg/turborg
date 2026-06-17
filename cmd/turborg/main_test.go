package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHealthcheckURLHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	require.NoError(t, healthcheckURL(srv.URL))
}

func TestHealthcheckURLUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	require.Error(t, healthcheckURL(srv.URL))
}

func TestHealthcheckURLUnreachable(t *testing.T) {
	require.Error(t, healthcheckURL("http://127.0.0.1:1"))
}
