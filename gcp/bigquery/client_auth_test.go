package bigquery

import (
	"context"
	"strings"
	"testing"
)

func TestNewClientRequiresFederation(t *testing.T) {
	t.Setenv("GCP_PROJECT_ID", "")
	t.Setenv("GCP_PROJECT_NUMBER", "")

	_, err := NewClient(context.Background(), "emrys-ops-prod", "dataset")
	if err == nil {
		t.Fatal("expected error without federation credentials")
	}
	if !strings.Contains(err.Error(), "IAM federation") {
		t.Errorf("got %v, want IAM federation error", err)
	}
}
