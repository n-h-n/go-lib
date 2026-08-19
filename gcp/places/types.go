package places

// AutocompleteRequest is one Places Autocomplete (New) call.
type AutocompleteRequest struct {
	Query        string
	State        string // 2-letter US region; appended to input and used as bias
	SessionToken string
	Limit        int
}

// Prediction is one Autocomplete suggestion (not yet a structured address).
type Prediction struct {
	PlaceID     string
	Description string
	MainText    string
	Secondary   string
}

// PlaceDetailsRequest fetches structured address fields for a prediction.
type PlaceDetailsRequest struct {
	PlaceID      string
	SessionToken string
}

// Address is a US mailing address extracted from Place Details components.
type Address struct {
	PlaceID      string
	Description  string
	AddressLine1 string
	AddressLine2 string
	City         string
	State        string
	PostalCode   string
}
