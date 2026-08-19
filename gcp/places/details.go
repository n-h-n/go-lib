package places

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

type detailsResponse struct {
	ID                string             `json:"id"`
	FormattedAddress  string             `json:"formattedAddress"`
	AddressComponents []addressComponent `json:"addressComponents"`
}

type addressComponent struct {
	LongText  string   `json:"longText"`
	ShortText string   `json:"shortText"`
	Types     []string `json:"types"`
}

// PlaceDetails returns a structured US address for a Place ID.
func (c *Client) PlaceDetails(ctx context.Context, req PlaceDetailsRequest) (*Address, error) {
	if c == nil {
		return nil, nil
	}
	placeID := strings.TrimPrefix(strings.TrimSpace(req.PlaceID), "places/")
	if placeID == "" {
		return nil, fmt.Errorf("places: place id is required")
	}
	if err := c.wait(ctx, c.detailsLimit); err != nil {
		return nil, err
	}

	rawURL := fmt.Sprintf(placeURLFmt, escapePlaceID(placeID))
	if tok := strings.TrimSpace(req.SessionToken); tok != "" {
		rawURL += "?sessionToken=" + url.QueryEscape(tok)
	}

	var out detailsResponse
	if err := c.doJSON(ctx, "GET", rawURL, detailsFieldMask, nil, &out); err != nil {
		return nil, fmt.Errorf("places: details: %w", err)
	}
	addr := parseAddress(out)
	if addr.PlaceID == "" {
		addr.PlaceID = placeID
	}
	return addr, nil
}
