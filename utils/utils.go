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
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/slices"
	"golang.org/x/net/publicsuffix"

	"github.com/agnivade/levenshtein"

	"github.com/n-h-n/go-lib/aws/elasticache"
	"github.com/n-h-n/go-lib/log"
	"github.com/n-h-n/go-lib/redis/rlock"
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

func RemoveElementByIndex[T any](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}

// ExtractTopLevelDomain extracts the effective top-level domain (eTLD+1) from a website URL.
// It properly handles public suffixes like .ac.jp, .co.uk, etc.
// Examples:
//   - "http://www.nifs.ac.jp" -> "nifs.ac.jp"
//   - "http://english.cas.cn/" -> "cas.cn"
//   - "https://example.com" -> "example.com"
//
// Returns an empty string if the URL cannot be parsed or the domain cannot be extracted.
func ExtractTopLevelDomain(websiteURL string) string {
	if websiteURL == "" {
		return ""
	}

	var hostname string

	// Try parsing as URL first
	parsedURL, err := url.Parse(websiteURL)
	if err == nil && parsedURL.Host != "" {
		hostname = parsedURL.Host
	} else {
		// If parsing failed or no host, try to extract hostname manually
		// Remove common prefixes
		hostname = strings.TrimPrefix(websiteURL, "http://")
		hostname = strings.TrimPrefix(hostname, "https://")
		// Remove path and query
		if idx := strings.IndexAny(hostname, "/?#"); idx != -1 {
			hostname = hostname[:idx]
		}
	}

	// Remove port if present
	if idx := strings.Index(hostname, ":"); idx != -1 {
		hostname = hostname[:idx]
	}

	// Trim whitespace
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return ""
	}

	// Use publicsuffix to get the effective top-level domain + 1 label (eTLD+1)
	// This properly handles cases like .ac.jp, .co.uk, etc.
	domain, err := publicsuffix.EffectiveTLDPlusOne(hostname)
	if err != nil {
		// If publicsuffix fails, fall back to a simple extraction
		parts := strings.Split(hostname, ".")
		if len(parts) >= 2 {
			// Take last two parts as a simple fallback
			return strings.Join(parts[len(parts)-2:], ".")
		}
		return hostname
	}

	return domain
}

// BuildQueryString builds a query string from a map of parameters.
// It handles slices by adding multiple query params with the same key,
// and properly URL encodes all values.
func BuildQueryString(params map[string]interface{}) string {
	if len(params) == 0 {
		return ""
	}

	values := url.Values{}
	for key, value := range params {
		switch v := value.(type) {
		case []string:
			// Handle slice parameters by adding multiple query params
			for _, item := range v {
				values.Add(key, item)
			}
		case string:
			if v != "" {
				values.Add(key, v)
			}
		case int:
			if v > 0 {
				values.Add(key, strconv.Itoa(v))
			}
		case int64:
			if v > 0 {
				values.Add(key, strconv.FormatInt(v, 10))
			}
		case bool:
			if v {
				values.Add(key, "true")
			}
		default:
			// Handle other types by converting to string
			if v != nil {
				values.Add(key, fmt.Sprintf("%v", v))
			}
		}
	}

	queryString := values.Encode()
	if queryString != "" {
		return "?" + queryString
	}
	return ""
}

// RateLimiter interface that both local and Redis limiters implement
type RateLimiter interface {
	Allow(ctx context.Context) bool
	Wait(ctx context.Context) error
	WaitN(ctx context.Context, n int) error
	Close() error
}

// AdaptiveRateLimiter extends RateLimiter with synchronization methods for adaptive rate limiting
type AdaptiveRateLimiter interface {
	RateLimiter
	SyncFromHeaders(rateLimitInfo *RateLimitInfo) error
	GetCurrentState() *RateLimitState
}

// RateLimitState represents the current state of the rate limiter
type RateLimitState struct {
	RequestsLimit     int           `json:"requests_limit"`
	RequestsRemaining int           `json:"requests_remaining"`
	RetryAfter        time.Duration `json:"retry_after"`
	LastSync          time.Time     `json:"last_sync"`
}

// RateLimitInfo contains rate limiting information from API responses
type RateLimitInfo struct {
	RetryAfter        int `json:"retry_after"`
	RequestsLimit     int `json:"requests_limit"`
	RequestsRemaining int `json:"requests_remaining"`
}

// Wrapper types to adapt the different limiter interfaces
type redisLimiterWrapper struct {
	limiter  interface{}
	state    *RateLimitState
	lastSync time.Time
	mu       sync.RWMutex
}

func (w *redisLimiterWrapper) Allow(ctx context.Context) bool {
	// Use reflection or type assertion to call Allow
	// Both rlim.Limiter and llim.Limiter have Allow(ctx context.Context) bool
	if l, ok := w.limiter.(interface{ Allow(context.Context) bool }); ok {
		return l.Allow(ctx)
	}
	return false
}

func (w *redisLimiterWrapper) Wait(ctx context.Context) error {
	// Call Wait with no options - both limiters support variadic opts
	if l, ok := w.limiter.(interface {
		Wait(context.Context, ...interface{}) error
	}); ok {
		return l.Wait(ctx)
	}
	// Fallback: try calling with empty variadic args using reflection
	return w.callWait(ctx)
}

func (w *redisLimiterWrapper) WaitN(ctx context.Context, n int) error {
	if l, ok := w.limiter.(interface {
		WaitN(context.Context, int, ...interface{}) error
	}); ok {
		return l.WaitN(ctx, n)
	}
	return w.callWaitN(ctx, n)
}

func (w *redisLimiterWrapper) callWait(ctx context.Context) error {
	// Use reflection to call Wait with empty variadic args
	method := reflect.ValueOf(w.limiter).MethodByName("Wait")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have Wait method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *redisLimiterWrapper) callWaitN(ctx context.Context, n int) error {
	method := reflect.ValueOf(w.limiter).MethodByName("WaitN")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have WaitN method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(n)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *redisLimiterWrapper) Close() error {
	return nil
}

func (w *redisLimiterWrapper) SyncFromHeaders(rateLimitInfo *RateLimitInfo) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == nil {
		w.state = &RateLimitState{}
	}

	w.state.RequestsLimit = rateLimitInfo.RequestsLimit
	w.state.RequestsRemaining = rateLimitInfo.RequestsRemaining
	w.state.RetryAfter = time.Duration(rateLimitInfo.RetryAfter) * time.Second
	w.state.LastSync = time.Now()
	w.lastSync = time.Now()

	return nil
}

func (w *redisLimiterWrapper) GetCurrentState() *RateLimitState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == nil {
		return &RateLimitState{}
	}

	// Return a copy to avoid race conditions
	return &RateLimitState{
		RequestsLimit:     w.state.RequestsLimit,
		RequestsRemaining: w.state.RequestsRemaining,
		RetryAfter:        w.state.RetryAfter,
		LastSync:          w.state.LastSync,
	}
}

type localLimiterWrapper struct {
	limiter  interface{}
	state    *RateLimitState
	lastSync time.Time
	mu       sync.RWMutex
	// Adaptive rate limiting state
	adaptiveMode bool
}

func (w *localLimiterWrapper) Allow(ctx context.Context) bool {
	if l, ok := w.limiter.(interface{ Allow(context.Context) bool }); ok {
		return l.Allow(ctx)
	}
	return false
}

func (w *localLimiterWrapper) Wait(ctx context.Context) error {
	if l, ok := w.limiter.(interface {
		Wait(context.Context, ...interface{}) error
	}); ok {
		return l.Wait(ctx)
	}
	return w.callWait(ctx)
}

func (w *localLimiterWrapper) WaitN(ctx context.Context, n int) error {
	if l, ok := w.limiter.(interface {
		WaitN(context.Context, int, ...interface{}) error
	}); ok {
		return l.WaitN(ctx, n)
	}
	return w.callWaitN(ctx, n)
}

func (w *localLimiterWrapper) callWait(ctx context.Context) error {
	method := reflect.ValueOf(w.limiter).MethodByName("Wait")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have Wait method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *localLimiterWrapper) callWaitN(ctx context.Context, n int) error {
	method := reflect.ValueOf(w.limiter).MethodByName("WaitN")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have WaitN method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(n)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *localLimiterWrapper) Close() error {
	return nil
}

func (w *localLimiterWrapper) SyncFromHeaders(rateLimitInfo *RateLimitInfo) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == nil {
		w.state = &RateLimitState{}
	}

	w.state.RequestsLimit = rateLimitInfo.RequestsLimit
	w.state.RequestsRemaining = rateLimitInfo.RequestsRemaining
	w.state.RetryAfter = time.Duration(rateLimitInfo.RetryAfter) * time.Second
	w.state.LastSync = time.Now()
	w.lastSync = time.Now()

	// Enable adaptive mode if we have valid rate limit info
	if rateLimitInfo.RequestsLimit > 0 {
		w.adaptiveMode = true
	}

	return nil
}

func (w *localLimiterWrapper) GetCurrentState() *RateLimitState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.state == nil {
		return &RateLimitState{}
	}

	// Return a copy to avoid race conditions
	return &RateLimitState{
		RequestsLimit:     w.state.RequestsLimit,
		RequestsRemaining: w.state.RequestsRemaining,
		RetryAfter:        w.state.RetryAfter,
		LastSync:          w.state.LastSync,
	}
}

// Simple wrapper types without adaptive features
type simpleRedisLimiterWrapper struct {
	limiter interface{}
}

func (w *simpleRedisLimiterWrapper) Allow(ctx context.Context) bool {
	if l, ok := w.limiter.(interface{ Allow(context.Context) bool }); ok {
		return l.Allow(ctx)
	}
	return false
}

func (w *simpleRedisLimiterWrapper) Wait(ctx context.Context) error {
	if l, ok := w.limiter.(interface {
		Wait(context.Context, ...interface{}) error
	}); ok {
		return l.Wait(ctx)
	}
	return w.callWait(ctx)
}

func (w *simpleRedisLimiterWrapper) WaitN(ctx context.Context, n int) error {
	if l, ok := w.limiter.(interface {
		WaitN(context.Context, int, ...interface{}) error
	}); ok {
		return l.WaitN(ctx, n)
	}
	return w.callWaitN(ctx, n)
}

func (w *simpleRedisLimiterWrapper) callWait(ctx context.Context) error {
	method := reflect.ValueOf(w.limiter).MethodByName("Wait")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have Wait method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *simpleRedisLimiterWrapper) callWaitN(ctx context.Context, n int) error {
	method := reflect.ValueOf(w.limiter).MethodByName("WaitN")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have WaitN method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(n)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *simpleRedisLimiterWrapper) Close() error {
	return nil
}

type simpleLocalLimiterWrapper struct {
	limiter interface{}
}

func (w *simpleLocalLimiterWrapper) Allow(ctx context.Context) bool {
	if l, ok := w.limiter.(interface{ Allow(context.Context) bool }); ok {
		return l.Allow(ctx)
	}
	return false
}

func (w *simpleLocalLimiterWrapper) Wait(ctx context.Context) error {
	if l, ok := w.limiter.(interface {
		Wait(context.Context, ...interface{}) error
	}); ok {
		return l.Wait(ctx)
	}
	return w.callWait(ctx)
}

func (w *simpleLocalLimiterWrapper) WaitN(ctx context.Context, n int) error {
	if l, ok := w.limiter.(interface {
		WaitN(context.Context, int, ...interface{}) error
	}); ok {
		return l.WaitN(ctx, n)
	}
	return w.callWaitN(ctx, n)
}

func (w *simpleLocalLimiterWrapper) callWait(ctx context.Context) error {
	method := reflect.ValueOf(w.limiter).MethodByName("Wait")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have Wait method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *simpleLocalLimiterWrapper) callWaitN(ctx context.Context, n int) error {
	method := reflect.ValueOf(w.limiter).MethodByName("WaitN")
	if !method.IsValid() {
		return fmt.Errorf("limiter does not have WaitN method")
	}
	args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(n)}
	result := method.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		return result[0].Interface().(error)
	}
	return nil
}

func (w *simpleLocalLimiterWrapper) Close() error {
	return nil
}

// NewRedisRateLimiterWrapper creates a Redis rate limiter wrapper.
// The limiter parameter should be rlim.Limiter.
// If adaptive is true, returns an AdaptiveRateLimiter.
func NewRedisRateLimiterWrapper(limiter interface{}, adaptive bool) RateLimiter {
	if adaptive {
		return &redisLimiterWrapper{limiter: limiter}
	}
	return &simpleRedisLimiterWrapper{limiter: limiter}
}

// NewLocalRateLimiterWrapper creates a local rate limiter wrapper.
// The limiter parameter should be llim.Limiter.
// If adaptive is true, returns an AdaptiveRateLimiter.
func NewLocalRateLimiterWrapper(limiter interface{}, adaptive bool) RateLimiter {
	if adaptive {
		return &localLimiterWrapper{limiter: limiter}
	}
	return &simpleLocalLimiterWrapper{limiter: limiter}
}

// DistributedLock interface for distributed locking (local or Redis-based)
type DistributedLock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

// localLockWrapper is a no-op lock implementation for local development
type localLockWrapper struct{}

func (l *localLockWrapper) Lock(ctx context.Context) error {
	return nil
}

func (l *localLockWrapper) Unlock(ctx context.Context) error {
	return nil
}

// redisLockWrapper wraps rlock.Mutex for Redis-based distributed locking
type redisLockWrapper struct {
	mtx rlock.Mutex
}

func (l *redisLockWrapper) Lock(ctx context.Context) error {
	return l.mtx.Lock(ctx)
}

func (l *redisLockWrapper) Unlock(ctx context.Context) error {
	return l.mtx.Unlock(ctx)
}

// NewDistributedLock creates a distributed lock based on available resources.
// It accepts an elasticache client, keyspace, appID, verboseMode,
// and optional rlock.MutexOpt options.
// If elasticache client is nil or unavailable, returns a local no-op lock.
func NewDistributedLock(
	elasticacheClient *elasticache.Client,
	keyspace string,
	appID string,
	verboseMode bool,
	opts ...rlock.MutexOpt,
) DistributedLock {
	// Use Redis-based locking if elasticache client is available
	if elasticacheClient != nil {
		client := elasticacheClient.GetRedisClient()
		if client != nil {
			rlockOpts := append([]rlock.MutexOpt{rlock.WithWait(false)}, opts...)
			if verboseMode {
				rlockOpts = append(rlockOpts, rlock.WithVerbose())
			}
			mtx := rlock.NewMutex(client, keyspace, appID, rlockOpts...)
			return &redisLockWrapper{mtx: mtx}
		}
	}

	// Use local no-op lock for development or when Redis is unavailable
	return &localLockWrapper{}
}
