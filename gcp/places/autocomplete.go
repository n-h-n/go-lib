package places

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

type autocompleteBody struct {
	Input               string   `json:"input"`
	IncludedRegionCodes []string `json:"includedRegionCodes,omitempty"`
	LanguageCode        string   `json:"languageCode,omitempty"`
	RegionCode          string   `json:"regionCode,omitempty"`
	SessionToken        string   `json:"sessionToken,omitempty"`
}

type localizedText struct {
	Text string `json:"text"`
}

type structuredFormat struct {
	MainText      localizedText `json:"mainText"`
	SecondaryText localizedText `json:"secondaryText"`
}

type placePrediction struct {
	PlaceID          string           `json:"placeId"`
	Text             localizedText    `json:"text"`
	StructuredFormat structuredFormat `json:"structuredFormat"`
}

type autocompleteSuggestion struct {
	PlacePrediction *placePrediction `json:"placePrediction"`
}

type autocompleteResponse struct {
	Suggestions []autocompleteSuggestion `json:"suggestions"`
}

const (
	defaultSuggestLimit = 8
	maxSuggestLimit     = 12
	minQueryLen         = 3
)

// Autocomplete returns place predictions for a street query in one US state.
func (c *Client) Autocomplete(ctx context.Context, req AutocompleteRequest) ([]Prediction, error) {
	if c == nil {
		return nil, nil
	}
	query := strings.TrimSpace(req.Query)
	state := normalizeState(req.State)
	if len(query) < minQueryLen || len(state) != 2 {
		return nil, nil
	}
	if err := c.wait(ctx, c.autocompleteLimit); err != nil {
		return nil, err
	}

	input := query
	if !strings.Contains(strings.ToUpper(query), state) {
		input = query + ", " + state
	}

	var out autocompleteResponse
	body := autocompleteBody{
		Input:               input,
		IncludedRegionCodes: []string{"us"},
		LanguageCode:        "en",
		RegionCode:          "US",
		SessionToken:        strings.TrimSpace(req.SessionToken),
	}
	if err := c.doJSON(ctx, "POST", autocompleteURL, autocompleteFieldMask, body, &out); err != nil {
		return nil, fmt.Errorf("places: autocomplete: %w", err)
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultSuggestLimit
	}
	if limit > maxSuggestLimit {
		limit = maxSuggestLimit
	}

	preds := make([]Prediction, 0, len(out.Suggestions))
	for _, s := range out.Suggestions {
		if s.PlacePrediction == nil {
			continue
		}
		p := s.PlacePrediction
		placeID := strings.TrimPrefix(strings.TrimSpace(p.PlaceID), "places/")
		if placeID == "" {
			continue
		}
		desc := strings.TrimSpace(p.Text.Text)
		if desc == "" {
			desc = strings.TrimSpace(p.StructuredFormat.MainText.Text)
		}
		preds = append(preds, Prediction{
			PlaceID:     placeID,
			Description: desc,
			MainText:    strings.TrimSpace(p.StructuredFormat.MainText.Text),
			Secondary:   strings.TrimSpace(p.StructuredFormat.SecondaryText.Text),
		})
		if len(preds) >= limit {
			break
		}
	}
	return preds, nil
}

func normalizeState(s string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(s) {
		if unicode.IsLetter(r) {
			b.WriteRune(unicode.ToUpper(r))
		}
	}
	out := b.String()
	if len(out) != 2 {
		return ""
	}
	return out
}
