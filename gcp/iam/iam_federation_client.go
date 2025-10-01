package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSTS "github.com/aws/aws-sdk-go-v2/service/sts"
	"golang.org/x/oauth2"
	gcpIAM "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"
	gcpSTS "google.golang.org/api/sts/v1"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

type IdentityFederationClient struct {
	aws          *awsSvc
	serviceName  string
	env          string
	gcp          *gcpSvc
	iam          iam.IAMClient
	opt          *clientOpts
	refreshMutex *sync.Mutex
}

type gcpSubjectTokenFormat struct {
	Url     string              `json:"url"`
	Method  string              `json:"method"`
	Headers []map[string]string `json:"headers"`
}

type awsSvc struct {
	assumedRole                  *awsSTS.AssumeRoleOutput
	getCallerIdentityEndpointUrl string
	region                       string
	roleARN                      string
	service                      string
	sessionName                  string
	signedHeader                 *http.Header
	stsHost                      string
	tokenUpdateTime              time.Time
}

type gcpSvc struct {
	audience                      string
	addedScopes                   []string
	federationTokenResponse       *gcpSTS.GoogleIdentityStsV1ExchangeTokenResponse
	federationTokenUpdateTime     time.Time
	projectID                     string
	projectNumber                 string
	serviceAccountEmail           string
	serviceAccountName            string
	serviceAccountTokenExpireTime time.Time
	serviceAccountTokenResponse   *gcpIAM.GenerateAccessTokenResponse
	serviceAccountTokenUpdateTime time.Time
	subjectToken                  *http.Request
	tokenSource                   *TokenSource
}

type clientOpts struct {
	flagRefreshAlreadySet  bool
	periodicAuthCancelFunc func()
	periodicAuthRefresh    bool
	periodicAuthRunning    bool
	refreshPercentage      float64
	setADC                 bool
	tokenDurationSeconds   int64
	verbose                bool
}

type TokenSource struct {
	token *oauth2.Token
	Type  string `json:"type,omitempty"`
}

// Creates a simple IAM federation client to handle AWS and GCP tokens.
func NewIdentityFederationClient(ctx context.Context, gcpProjectID, gcpProjectNum string, options ...Option) (*IdentityFederationClient, error) {
	c := IdentityFederationClient{
		aws: &awsSvc{
			service:                      "sts",
			getCallerIdentityEndpointUrl: "https://sts." + os.Getenv(AWS_REGION) + ".amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
			stsHost:                      "sts." + os.Getenv(AWS_REGION) + ".amazonaws.com",
		},
		env: strings.ToLower(os.Getenv(ACTIVE_ENV)),
		gcp: &gcpSvc{
			addedScopes:                 []string{},
			projectID:                   gcpProjectID,
			projectNumber:               gcpProjectNum,
			serviceAccountTokenResponse: new(gcpIAM.GenerateAccessTokenResponse),
			tokenSource:                 &TokenSource{},
		},
		opt: &clientOpts{
			flagRefreshAlreadySet: false,
			periodicAuthRefresh:   true,
			periodicAuthRunning:   false,
			refreshPercentage:     0.20,
			tokenDurationSeconds:  3600,
		},
		refreshMutex: new(sync.Mutex),
	}

	// options
	for _, option := range options {
		err := option(&c)
		if err != nil {
			return nil, err
		}
	}

	if c.env == "" {
		c.env = "local"
	}

	if c.aws.sessionName == "" {
		c.aws.sessionName = "gcp-federation"
	}

	iamClient, err := iam.NewIAMClient(ctx, iam.WithSessionName(c.aws.sessionName))
	if err != nil {
		return nil, err
	}
	c.iam = iamClient
	c.aws.assumedRole = c.iam.GetAssumedRole()
	c.aws.tokenUpdateTime = c.iam.GetTokenUpdateTime()

	c.aws.roleARN = c.iam.GetRoleARN()
	c.aws.region = c.iam.GetAWSRegion()
	// c.aws.roleARN should always look like arn:aws:iam::{aws_account_id}:role/{service_name},
	// if your service has some other format of an IAM role name, it is specified incorrectly in
	// starbase and it's advised you fix it.
	// e.g. correct role name format: arn:aws:iam::020040093233:role/janus
	roleARNSplit := strings.Split(c.aws.roleARN, "/")
	c.serviceName = roleARNSplit[len(roleARNSplit)-1]

	if c.gcp.projectID == "" || c.gcp.projectNumber == "" {
		// No override was given, and so check the environment variables.

		gcpProjectID := os.Getenv(GCP_PROJECT_ID)
		gcpProjectNumber := os.Getenv(GCP_PROJECT_NUMBER)
		if gcpProjectID == "" || gcpProjectNumber == "" {
			log.Log.Fatal(ctx, "both GCP_PROJECT_ID and GCP_PROJECT_NUMBER must be set in env vars")
		}
		c.gcp.projectID = gcpProjectID
		c.gcp.projectNumber = gcpProjectNumber
	}

	if c.gcp.serviceAccountName == "" {
		// First check if the service name is specified in the environment
		// variable, otherwise use the serviceName
		if os.Getenv(SERVICE_NAME) != "" {
			c.gcp.serviceAccountName = os.Getenv(SERVICE_NAME)
		} else {
			c.gcp.serviceAccountName = c.serviceName
		}
	}

	c.gcp.audience = fmt.Sprintf("//iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider", c.gcp.projectNumber)
	c.gcp.serviceAccountEmail = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", c.gcp.serviceAccountName, c.gcp.projectID)

	// last point before firing off requests to AWS and GCP to initialize client with tokens
	// if verbose, we log some key pieces of info that should be expected
	if c.opt.verbose {
		log.Log.Debugf(ctx, `
		 using: env=%s, awsRoleARN=%s, awsRegion=%s, serviceName=%s, gcpProjectId=%s, gcpProjectNumber=%s, gcpAudience=%s, gcpServiceAccountEmail=%s, tokenDurationSeconds=%v
		 `, c.env, c.aws.roleARN, c.aws.region, c.serviceName, c.gcp.projectID, c.gcp.projectNumber, c.gcp.audience, c.gcp.serviceAccountEmail, c.opt.tokenDurationSeconds)
	}

	err = c.authenticate(ctx)

	if err != nil {
		return nil, err
	}

	if c.opt.periodicAuthRefresh {
		_, err := c.StartPeriodicAuthRefresh(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &c, nil
}

func (c *IdentityFederationClient) generateSignedHeader(ctx context.Context) error {
	if c.opt.verbose {
		log.Log.Info(ctx, "generating signed v4 authenticated AWS request....")
	}

	c.aws.assumedRole = c.iam.GetAssumedRole()
	c.aws.tokenUpdateTime = c.iam.GetTokenUpdateTime()

	signer := v4.NewSigner()
	req, err := http.NewRequest(http.MethodPost, c.aws.getCallerIdentityEndpointUrl, nil)
	if err != nil {
		return err
	}

	req.Header.Set("x-amz-date", c.aws.tokenUpdateTime.Format("20060102T150405Z"))
	req.Header.Set("x-amz-security-token", *c.aws.assumedRole.Credentials.SessionToken)
	req.Header.Set("x-goog-cloud-target-resource", c.gcp.audience)

	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		*c.aws.assumedRole.Credentials.AccessKeyId,
		*c.aws.assumedRole.Credentials.SecretAccessKey,
		*c.aws.assumedRole.Credentials.SessionToken,
	)

	awsCreds, err := awsCredsProvider.Retrieve(ctx)
	if err != nil {
		return err
	}

	err = signer.SignHTTP(
		ctx,
		awsCreds,
		req,
		iam.EmptyBodySHA256,
		c.aws.service,
		c.aws.region,
		c.aws.tokenUpdateTime,
	)
	if err != nil {
		log.Log.Errorf(ctx, "failed to acquire signed v4 AWS request to service %s for region %s with req: %v",
			c.aws.service,
			c.aws.region,
			req,
		)
		return err
	}

	c.aws.signedHeader = &req.Header
	c.gcp.subjectToken = req

	if c.opt.verbose {
		log.Log.Info(ctx, "successfully acquired signed AWS request")
	}
	return nil
}

func (c *IdentityFederationClient) getGCPFederationToken(ctx context.Context) error {
	if c.opt.verbose {
		log.Log.Info(ctx, "getting GCP federation token....")
	}

	// We create the GCP STS client here without auth needed, since we're using the
	// public token exchange endpoint with essentially an auth token that we're forming
	// below (the gcpSubjectTokenFormat{})
	stsService, err := gcpSTS.NewService(ctx, option.WithoutAuthentication())

	if err != nil {
		log.Log.Error(ctx, "failed to instantiate GCP STS service client")
		return err
	}

	token := gcpSubjectTokenFormat{
		Url:    c.aws.getCallerIdentityEndpointUrl,
		Method: c.gcp.subjectToken.Method,
		Headers: []map[string]string{
			{
				"key":   "host",
				"value": c.gcp.subjectToken.Host,
			},
			{
				"key":   "x-amz-date",
				"value": c.gcp.subjectToken.Header.Get("X-Amz-Date"),
			},
			{
				"key":   "x-amz-security-token",
				"value": c.gcp.subjectToken.Header.Get("X-Amz-Security-Token"),
			},
			{
				"key":   "x-goog-cloud-target-resource",
				"value": c.gcp.subjectToken.Header.Get("X-Goog-Cloud-Target-Resource"),
			},
			{
				"key":   "Authorization",
				"value": c.gcp.subjectToken.Header.Get("Authorization"),
			},
		},
	}

	tokenBytesBuffer := new(bytes.Buffer)
	enc := json.NewEncoder(tokenBytesBuffer)
	enc.SetEscapeHTML(false)
	err = enc.Encode(&token)

	if err != nil {
		log.Log.Errorf(ctx, "failed to encode token for GCP federation token exchange with request: %s", token)
		return err
	}

	tokenRequest := gcpSTS.GoogleIdentityStsV1ExchangeTokenRequest{
		Audience:           c.gcp.audience,
		GrantType:          "urn:ietf:params:oauth:grant-type:token-exchange",
		RequestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
		Scope:              "https://www.googleapis.com/auth/cloud-platform",
		SubjectTokenType:   "urn:ietf:params:aws:token-type:aws4_request",
		SubjectToken:       url.QueryEscape(tokenBytesBuffer.String()),
	}

	if c.opt.verbose {
		log.Log.Debugf(ctx, "GCP STS token request: %v", tokenRequest)
	}

	stsTokenCallResponse, err := stsService.V1.Token(&tokenRequest).Do()
	if err != nil {
		log.Log.Errorf(ctx, "failed on GCP STS token request with error %v using token request %v", err, tokenRequest)
		return err
	}

	c.gcp.federationTokenUpdateTime = time.Now().UTC()
	c.gcp.federationTokenResponse = stsTokenCallResponse

	if c.opt.verbose {
		log.Log.Info(ctx, "successfully acquired GCP federation token")
	}
	return nil
}

func (c *IdentityFederationClient) getGCPAccessToken(ctx context.Context) error {
	if c.opt.verbose {
		log.Log.Debugf(ctx, "getting service account access token for %s ....", c.serviceName)
	}
	iamService, err := gcpIAM.NewService(ctx, option.WithoutAuthentication())

	if err != nil {
		log.Log.Error(ctx, "failed to instantiate GCP IAM client")
		return err
	}

	scopes := []string{"https://www.googleapis.com/auth/cloud-platform"}
	scopes = append(scopes, c.gcp.addedScopes...)

	accessTokenRequest := &gcpIAM.GenerateAccessTokenRequest{
		Lifetime: strconv.FormatInt(c.opt.tokenDurationSeconds, 10) + "s",
		Scope:    scopes,
	}

	requestName := fmt.Sprintf("projects/-/serviceAccounts/%s", c.gcp.serviceAccountEmail)
	accessTokenCall := iamService.Projects.ServiceAccounts.GenerateAccessToken(requestName, accessTokenRequest)
	accessTokenCall.Header().Set("Content-Type", "text/json; charset=utf-8")
	accessTokenCall.Header().Set("Authorization", fmt.Sprintf("Bearer %s", c.gcp.federationTokenResponse.AccessToken))

	accessTokenResponse, err := accessTokenCall.Do()
	if err != nil {
		log.Log.Errorf(ctx, "failed to acquire service account token with request: %s", accessTokenRequest)
		return err
	}

	c.gcp.serviceAccountTokenUpdateTime = time.Now().UTC()
	c.gcp.serviceAccountTokenResponse = accessTokenResponse
	expirationTime, err := time.Parse("2006-01-02T15:04:05Z", c.gcp.serviceAccountTokenResponse.ExpireTime)
	if err != nil {
		return err
	}
	c.gcp.serviceAccountTokenExpireTime = expirationTime
	if c.opt.verbose {
		log.Log.Debugf(ctx, "successfully acquired GCP access token for %s", c.serviceName)
		log.Log.Debugf(ctx, "token expires: %s", c.gcp.serviceAccountTokenResponse.ExpireTime)
	}
	return nil
}

func (c *IdentityFederationClient) formOAuthToken(ctx context.Context) error {
	if c.opt.verbose {
		log.Log.Info(ctx, "forming OAuth 2.0 token from service access token ....")
	}

	c.gcp.tokenSource.token = &oauth2.Token{
		AccessToken: c.gcp.serviceAccountTokenResponse.AccessToken,
		TokenType:   "Bearer",
		Expiry:      c.gcp.serviceAccountTokenExpireTime,
	}

	if c.opt.verbose {
		log.Log.Info(ctx, "successfully formed OAuth 2.0 token")
	}
	return nil
}

func (t *TokenSource) Token() (*oauth2.Token, error) {
	return t.token, nil
}

// Returns a pointer to an implementation of oauth2.TokenSource for GCP's option.WithTokenSource() client authentication option
func (c *IdentityFederationClient) GetTokenSource(ctx context.Context) (*TokenSource, error) {
	if !c.opt.periodicAuthRefresh {
		if c.opt.verbose {
			log.Log.Info(ctx, "on-demand token authentication selected, checking time remaining")
		}

		if c.GetDurationUntilTokenExpiration(ctx).Seconds() < c.opt.refreshPercentage*float64(c.opt.tokenDurationSeconds) {
			if c.opt.verbose {
				log.Log.Info(ctx, "past threshold for refreshing auth; fetching new token")
			}
			err := c.authenticate(ctx)
			if err != nil {
				return nil, err
			}
		} else {
			if c.opt.verbose {
				log.Log.Debugf(ctx, "more than %v secoond remaining on GCP token", c.opt.refreshPercentage*float64(c.opt.tokenDurationSeconds))
			}
		}
	}
	return c.gcp.tokenSource, nil
}

// This is the main sequence of authentication to retrieve and exchange all the tokens.
func (c *IdentityFederationClient) authenticate(ctx context.Context) error {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()

	// If already holding a service account token, check if still greater than 20% of TTL,
	// otherwise refresh.
	timeRemaining := c.GetDurationUntilTokenExpiration(ctx).Seconds()
	if timeRemaining >= c.opt.refreshPercentage*float64(c.opt.tokenDurationSeconds) {
		if c.opt.verbose {
			log.Log.Debugf(ctx, "will not refresh token--above refreshPercentage; seconds still remaining: %vs; refresh threshold at %vs",
				timeRemaining,
				c.opt.refreshPercentage*float64(c.opt.tokenDurationSeconds),
			)
		}
		return nil
	}

	if c.opt.verbose {
		log.Log.Info(ctx, "beginning authentication with AWS and GCP for tokens")
	}

	// The service will assume its own role for temporary session credentials.
	err := c.iam.RefreshAWSCreds(ctx, iam.WithForceRefresh(true))
	if err != nil {
		return err
	}

	// Generates the AWS-signed header for GCP STS to validate
	err = c.generateSignedHeader(ctx)
	if err != nil {
		return err
	}

	// Creates a limited GCP federation token with the AWS signed header and session to GCP STS
	err = c.getGCPFederationToken(ctx)
	if err != nil {
		return err
	}
	// Creates the temporary GCP service account token to use in service API calls
	err = c.getGCPAccessToken(ctx)
	if err != nil {
		return err
	}

	// Forms the OAuth token
	err = c.formOAuthToken(ctx)
	if err != nil {
		return err
	}

	// Optionally sets the Application Default Credentials (ADC) for the GCP client
	if c.opt.setADC {
		err = c.SetADC(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Gets the duration remaining until the access token expires in the measurement specified. E.g. if measurement is time.Second,
// then this function returns the seconds until access token expiration.
func (c *IdentityFederationClient) GetDurationUntilTokenExpiration(ctx context.Context) time.Duration {
	if c.gcp.serviceAccountTokenResponse.ExpireTime == "" {
		return time.Duration(-1)
	}
	duration := time.Until(c.gcp.serviceAccountTokenExpireTime)

	return duration
}

// Spawns a goroutine to periodically check and refresh all AWS and GCP tokens when needed. Note that this
// function is called when the IdentityFederationClient is initialized with defaults and usually does
// not need to be called unless your service wants to set its own period of when the token
// refreshes will automatically happen.
func (c *IdentityFederationClient) StartPeriodicAuthRefresh(ctx context.Context) (cancel func(), err error) {
	if c.opt.periodicAuthRefresh && c.opt.periodicAuthRunning {
		if c.opt.verbose {
			log.Log.Info(ctx, "periodic authentication refresh is already set and running")
		}
		return func() {}, nil
	}

	if c.opt.verbose {
		log.Log.Info(ctx, "starting periodic authentication refresh")
	}

	ctx, cancel = context.WithCancel(ctx)
	go c.runPeriodicAuthRefresh(ctx)
	c.opt.periodicAuthRefresh = true
	c.opt.periodicAuthRunning = true
	c.opt.periodicAuthCancelFunc = cancel

	return cancel, nil
}

func (c *IdentityFederationClient) runPeriodicAuthRefresh(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Minute)

	for {
		select {
		case <-ticker.C:
			err := c.authenticate(ctx)
			if err != nil {
				log.Log.Errorf(ctx, "periodic authentication refresh did not successfully complete: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Sends the cancel signal to the periodic authentication refresh channel.
func (c *IdentityFederationClient) StopPeriodicAuthRefresh(ctx context.Context) error {
	if c.opt.periodicAuthCancelFunc == nil {
		log.Log.Info(ctx, "no periodic authentication is running to stop")
		return nil
	}

	if c.opt.verbose {
		log.Log.Info(ctx, "stopping periodic authentication refresh")
	}

	c.opt.periodicAuthCancelFunc()
	c.opt.periodicAuthRunning = false
	c.opt.periodicAuthCancelFunc = nil

	return nil
}

// Returns the GCP project ID for the environment.
func (c *IdentityFederationClient) GetProjectID() string {
	return c.gcp.projectID
}

// Paradigmatic client closure, for now only stops periodic auth refresh
func (c *IdentityFederationClient) Close(ctx context.Context) error {
	return c.StopPeriodicAuthRefresh(ctx)
}

func (c *IdentityFederationClient) SetADC(ctx context.Context) error {
	tokenSource, err := c.GetTokenSource(ctx)
	if err != nil {
		return err
	}

	tokenSource.Type = "service_account"

	jsonData, err := json.Marshal(tokenSource)
	if err != nil {
		return err
	}

	filePath := "./gcp-oauth.json"
	err = os.WriteFile(filePath, jsonData, 0644)
	if err != nil {
		return err
	}

	err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filePath)
	if err != nil {
		return err
	}

	if c.opt.verbose {
		log.Log.Info(ctx, "successfully set ADC")
	}

	return nil

}
