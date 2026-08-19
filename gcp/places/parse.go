package places

import "strings"

func parseAddress(d detailsResponse) *Address {
	addr := &Address{
		PlaceID:     strings.TrimPrefix(strings.TrimSpace(d.ID), "places/"),
		Description: strings.TrimSpace(d.FormattedAddress),
	}
	var streetNumber, route, subpremise, locality, admin1, postal string
	for _, c := range d.AddressComponents {
		types := make(map[string]bool, len(c.Types))
		for _, t := range c.Types {
			types[t] = true
		}
		switch {
		case types["street_number"]:
			streetNumber = firstNonEmpty(c.LongText, c.ShortText)
		case types["route"]:
			route = firstNonEmpty(c.ShortText, c.LongText)
		case types["subpremise"]:
			subpremise = firstNonEmpty(c.LongText, c.ShortText)
		case types["locality"]:
			locality = firstNonEmpty(c.LongText, c.ShortText)
		case types["sublocality_level_1"] && locality == "":
			locality = firstNonEmpty(c.LongText, c.ShortText)
		case types["administrative_area_level_1"]:
			admin1 = strings.ToUpper(strings.TrimSpace(firstNonEmpty(c.ShortText, c.LongText)))
			if len(admin1) > 2 {
				admin1 = admin1[:2]
			}
		case types["postal_code"]:
			postal = firstNonEmpty(c.ShortText, c.LongText)
		}
	}
	addr.AddressLine1 = strings.TrimSpace(strings.Join(filterEmpty(streetNumber, route), " "))
	addr.AddressLine2 = strings.TrimSpace(subpremise)
	addr.City = strings.TrimSpace(locality)
	addr.State = admin1
	addr.PostalCode = strings.TrimSpace(postal)
	return addr
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func filterEmpty(vals ...string) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			out = append(out, s)
		}
	}
	return out
}
