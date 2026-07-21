package rds_postgres

import "testing"

func TestValidatePGIdent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		ident   string
		wantErr bool
	}{
		{name: "simple", ident: "daemon", wantErr: false},
		{name: "underscore", ident: "emrys_phi", wantErr: false},
		{name: "empty", ident: "", wantErr: true},
		{name: "injection", ident: "daemon; drop table", wantErr: true},
		{name: "dash", ident: "emrys-phi", wantErr: true},
		{name: "starts with digit", ident: "1daemon", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validatePGIdent(tc.ident)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q", tc.ident)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.ident, err)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	t.Parallel()
	got, err := quoteIdent("daemon")
	if err != nil {
		t.Fatal(err)
	}
	if got != `"daemon"` {
		t.Fatalf("got %s want \"daemon\"", got)
	}
}
