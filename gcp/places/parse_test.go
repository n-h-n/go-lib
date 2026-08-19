package places

import "testing"

func TestParseAddress(t *testing.T) {
	got := parseAddress(detailsResponse{
		ID:               "places/ChIJ123",
		FormattedAddress: "123 Main St, Apt 4, Madison, WI 53703, USA",
		AddressComponents: []addressComponent{
			{LongText: "123", ShortText: "123", Types: []string{"street_number"}},
			{LongText: "Main Street", ShortText: "Main St", Types: []string{"route"}},
			{LongText: "Apt 4", ShortText: "4", Types: []string{"subpremise"}},
			{LongText: "Madison", ShortText: "Madison", Types: []string{"locality", "political"}},
			{LongText: "Wisconsin", ShortText: "WI", Types: []string{"administrative_area_level_1", "political"}},
			{LongText: "53703", ShortText: "53703", Types: []string{"postal_code"}},
		},
	})
	if got.PlaceID != "ChIJ123" {
		t.Errorf("place id = %q", got.PlaceID)
	}
	if got.AddressLine1 != "123 Main St" {
		t.Errorf("line1 = %q", got.AddressLine1)
	}
	if got.AddressLine2 != "Apt 4" {
		t.Errorf("line2 = %q", got.AddressLine2)
	}
	if got.City != "Madison" || got.State != "WI" || got.PostalCode != "53703" {
		t.Errorf("city/state/zip = %q %q %q", got.City, got.State, got.PostalCode)
	}
}

func TestNormalizeState(t *testing.T) {
	if got := normalizeState(" wi "); got != "WI" {
		t.Errorf("got %q", got)
	}
	if got := normalizeState("Wisconsin"); got != "" {
		t.Errorf("full name should reject, got %q", got)
	}
}
