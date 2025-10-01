package utils

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/slices"

	"github.com/agnivade/levenshtein"

	"github.com/n-h-n/go-lib/log"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func HTTPGet(url string, opts ...func(*http.Request)) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(req)
	}

	client := http.Client{}
	return client.Do(req)
}

func HTTPJsonResp(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("content-type", "application/json")
	resp := make(map[string]string)
	resp["message"] = message
	if statusCode != http.StatusOK {
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(resp)
	} else {
		json.NewEncoder(w).Encode(resp)
	}
}

func Substr(s string, start int, length int) string {
	if start < 0 {
		start = 0
	}
	end := start + length
	if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}

func LogStruct(s interface{}) {
	val := reflect.ValueOf(s)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldName := val.Type().Field(i).Name
		fieldType := field.Type()

		// If the field is a struct, log its fields recursively
		if fieldType.Kind() == reflect.Struct {
			fmt.Printf("%s (%s):\n", fieldName, fieldType.Name())
			LogStruct(field.Interface())
		} else {
			fmt.Printf("%s: %v\n", fieldName, field.Interface())
		}
	}
}

// Posts to an HTTP endpoint with a JSON payload and returns the response as a map[string]interface{}
func HTTPPostJson(url string, payload interface{}) (map[string]interface{}, error) {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}

	var respMap map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&respMap)
	if err != nil {
		return nil, err
	}

	return respMap, nil
}

func PrintPrettyJSON(data map[string]interface{}) {
	prettyJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println(string(prettyJSON))
}

func GetDevOrgIDFromString(s string, fullID bool) string {
	if !fullID {
		re := regexp.MustCompile(`devo/([^:]+)`)
		match := re.FindStringSubmatch(s)
		if len(match) > 1 {
			return match[1]
		} else {
			return ""
		}
	} else {
		p := strings.Split(s, ":devu/")
		return p[0]
	}
}

func IndividualizeSetFields(obj interface{}, fieldsToIgnore []string, customOperators bson.M) (bson.D, error) {
	keys := bson.D{}

	bsonData, err := bson.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var bsonObj bson.D
	err = bson.Unmarshal(bsonData, &bsonObj)
	if err != nil {
		return nil, err
	}

	keys = append(keys, bsonObj...)

	for _, key := range fieldsToIgnore {
		for idx, elem := range keys {
			if elem.Key == key {
				keys = append(keys[:idx], keys[idx+1:]...)
				break
			}
		}
	}

	out := bson.D{
		{Key: "$set", Value: keys},
	}

	for k, v := range customOperators {
		out = append(out, bson.E{Key: k, Value: v})
	}

	return out, nil
}

func PeriodicRunFunc(ctx context.Context, f func() ([]byte, error), interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if b, err := f(); err != nil {
		log.Log.Errorf(ctx, "Error: %v\nStack Trace:\n%s", err, string(b))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b, err := f(); err != nil {
				log.Log.Errorf(ctx, "Error: %v\nStack Trace:\n%s", err, string(b))
			}
		}
	}
}

// GetSliceDifference returns the set difference between two slices of strings.
func GetSliceDifference(slice1, slice2 []string) []string {
	// Create a map to store the elements of slice2
	set2 := make(map[string]bool)
	for _, item := range slice2 {
		set2[item] = true
	}

	// Create a slice to store the difference
	difference := []string{}

	// Iterate through slice1 and check if each element is in slice2
	for _, item := range slice1 {
		if !set2[item] && !slices.Contains(difference, item) {
			difference = append(difference, item)
		}
	}

	return difference
}

func RemoveDuplicatesStr(inp []string) []string {
	elems := make(map[string]bool)
	l := []string{}
	for _, item := range inp {
		if _, value := elems[item]; !value {
			elems[item] = true
			l = append(l, item)
		}
	}
	return l
}

func FindFirstLetterIndex(s string) int {
	for i, char := range s {
		if unicode.IsLetter(char) {
			return i
		}
	}
	return -1 // Return -1 if no letter is found in the string
}

func StartFromFirstLetter(s string, opt ...func(*string)) string {
	idx := FindFirstLetterIndex(s)
	if idx == -1 {
		return ""
	}
	out := s[idx:]
	for _, f := range opt {
		f(&out)
	}
	return out
}

func WithCapitalizedLetter() func(*string) {
	return func(s *string) {
		*s = strings.ToUpper(string((*s)[0])) + (*s)[1:]
	}
}

func WithLowercase() func(*string) {
	return func(s *string) {
		*s = strings.ToLower(*s)
	}
}

func Chunk[T any](arr []T, size int) [][]T {
	chunks := make([][]T, (len(arr)+size-1)/size)
	for i := 0; i < len(arr); i += size {
		end := i + size
		if end > len(arr) {
			end = len(arr)
		}
		chunks[i/size] = arr[i:end]
	}
	return chunks
}

func RemoveElement[T any](s []T, r T) []T {
	for i := len(s) - 1; i >= 0; i-- {
		if reflect.DeepEqual(s[i], r) {
			s = append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func Unique[T comparable](s []T) []T {
	keys := make(map[T]bool)
	list := []T{}
	for _, entry := range s {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func CreateHMAC(message, key string, opts ...func(*hmacOpt)) string {
	hmacOptions := &hmacOpt{
		hashFunc: sha256.New,
	}

	for _, opt := range opts {
		opt(hmacOptions)
	}

	h := hmac.New(hmacOptions.hashFunc, []byte(key))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

func VerifyHMAC(message, key, expectedMAC string, opts ...func(*hmacOpt)) bool {
	mac := CreateHMAC(message, key, opts...)
	return hmac.Equal([]byte(mac), []byte(expectedMAC))
}

func withHashFunc(hf func() hash.Hash) func(*hmacOpt) {
	return func(o *hmacOpt) {
		o.hashFunc = hf
	}
}

type hmacOpt struct {
	hashFunc func() hash.Hash
}

func GetKeysFromMap[T any](m map[string]T) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GenerateUniqueHash(inputs ...string) string {
	concatenatedInputs := strings.ToLower(strings.Join(inputs, ""))

	h1 := sha256.New()
	h1.Write([]byte(concatenatedInputs))
	hash1 := base64.StdEncoding.EncodeToString(h1.Sum(nil))

	h2 := sha512.New384()
	h2.Write([]byte(concatenatedInputs))
	hash2 := base64.StdEncoding.EncodeToString(h2.Sum(nil))

	return hash1[len(hash1)-4:] + hash2[len(hash2)-4:]
}

func CalculatePercentageDiscount(a, b float64) float64 {
	var lower, higher float64
	if a < b {
		lower, higher = a, b
	} else {
		lower, higher = b, a
	}

	if lower == higher || higher == 0 {
		return 0
	}

	percentage := (lower / higher) * 100
	return 100 - math.Round(percentage*100)/100
}

func BoolToYesNo(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

func IsValidJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func GetRandomElement(slice []string) string {
	return slice[rand.Intn(len(slice))]
}

func GetRandomElements(slice []string, k int) []string {
	if k > len(slice) {
		return slice
	}

	if k <= 0 {
		return []string{}
	}

	n := len(slice)
	for i := 0; i < k; i++ {
		j := rand.Intn(n-i) + i
		slice[i], slice[j] = slice[j], slice[i]
	}

	return slice[:k]
}

func InterfaceToSliceString(data interface{}) ([]string, error) {
	slice, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not of type []interface{}")
	}

	result := make([]string, len(slice))
	for i, v := range slice {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("element at index %d is not a string", i)
		}
		result[i] = str
	}
	return result, nil
}

// CompareStringSlices checks if two slices of strings are exactly equivalent
func CompareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Checks to see if the email string is a valid email address
func IsValidEmail(email string) bool {
	re := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if re.MatchString(email) {
		return true
	} else {
		return false
	}
}

// SetStructField sets a field in a struct using a string key that matches the json tag
func SetStructField(obj interface{}, key string, value interface{}) error {
	v := reflect.ValueOf(obj)

	// If it's a pointer, resolve its value
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return fmt.Errorf("obj must be a struct or a pointer to a struct")
	}

	t := v.Type()
	// Remove tnt__ prefix if present
	searchKey := strings.TrimPrefix(key, "tnt__")

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Get the json tag
		tag := field.Tag.Get("json")
		if tag == "" {
			continue
		}

		// Split the tag to handle options like omitempty
		tagParts := strings.Split(tag, ",")
		jsonKey := tagParts[0]

		if jsonKey == "-" {
			continue
		}

		if jsonKey == searchKey {
			if !fieldValue.CanSet() {
				return fmt.Errorf("cannot set field %s", key)
			}

			val := reflect.ValueOf(value)
			if fieldValue.Type() != val.Type() {
				// Try to convert the value if possible
				if val.Type().ConvertibleTo(fieldValue.Type()) {
					val = val.Convert(fieldValue.Type())
				} else {
					return fmt.Errorf("value type %v is not assignable to field type %v", val.Type(), fieldValue.Type())
				}
			}

			fieldValue.Set(val)
			return nil
		}
	}

	return fmt.Errorf("no field found with json tag matching key: %s", searchKey)
}

// ConvertEmbeddingToBase64 converts []float32 or []float64 to a base64 string
func ConvertEmbeddingToBase64[T float32 | float64](embedding []T) (string, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, embedding)
	if err != nil {
		return "", fmt.Errorf("binary write error: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// ConvertBase64ToEmbedding converts a base64 string back to []float32 or []float64
func ConvertBase64ToEmbedding[T float32 | float64](str string) ([]T, error) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("base64 decode error: %w", err)
	}

	var zero T
	bytesPerFloat := binary.Size(zero)
	floats := make([]T, len(data)/bytesPerFloat)

	buf := bytes.NewReader(data)
	err = binary.Read(buf, binary.LittleEndian, floats)
	if err != nil {
		return nil, fmt.Errorf("binary read error: %w", err)
	}
	return floats, nil
}

// Outputs a string of the form [item1, item2, item3], with strings quoted and other types not quoted
func SliceToBracketedString[T any](items []T) string {
	bracketedStrings := make([]string, len(items))
	for i, item := range items {
		switch any(item).(type) {
		case string:
			stringItem := any(item).(string)
			bracketedStrings[i] = fmt.Sprintf("%q", stringItem)
		default:
			bracketedStrings[i] = fmt.Sprint(item)
		}
	}
	return fmt.Sprintf("[%s]", strings.Join(bracketedStrings, ", "))
}

func levenshteinNormalize(name string) string {
	// Trim and convert to lowercase
	normalized := strings.ToLower(strings.TrimSpace(name))

	// Remove punctuation
	normalized = strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) {
			return -1 // Remove the character
		}
		return r
	}, normalized)

	// Remove extra whitespace
	normalized = strings.Join(strings.Fields(normalized), " ")

	return normalized
}

func LevenshteinSimilarity(s1, s2 string) float64 {
	if len(s1) == 0 || len(s2) == 0 {
		return 0
	}

	norm1 := levenshteinNormalize(s1)
	norm2 := levenshteinNormalize(s2)

	distance := levenshtein.ComputeDistance(norm1, norm2)
	maxLen := math.Max(float64(len(norm1)), float64(len(norm2)))
	similarity := 1 - (float64(distance) / maxLen)

	return similarity
}
