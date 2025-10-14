package elasticache

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/n-h-n/go-lib/aws/iam"
)

const (
	requestProtocol        = "http://"
	paramAction            = "Action"
	paramUser              = "User"
	actionName             = "connect"
	serviceName            = "elasticache"
	paramExpires           = "X-Amz-Expires"
	tokenExpirationSeconds = 899
)

type IAMAuthTokenRequest struct {
	ctx                context.Context
	userID             string
	replicationGroupID string
	region             string
}

func (i *IAMAuthTokenRequest) toSignedRequestURI(credential aws.Credentials) (string, error) {
	req, err := i.getSignableRequest()
	if err != nil {
		return "", err
	}

	signedURI, _, err := i.sign(req, credential)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(signedURI)
	if err != nil {
		return "", err
	}

	res := url.URL{
		Scheme:   "http",
		Host:     u.Host,
		Path:     "/",
		RawQuery: u.RawQuery,
	}

	return strings.Replace(res.String(), requestProtocol, "", 1), nil
}

func (i *IAMAuthTokenRequest) getSignableRequest() (*http.Request, error) {
	query := url.Values{
		paramAction:  {actionName},
		paramUser:    {i.userID},
		paramExpires: {strconv.FormatInt(int64(tokenExpirationSeconds), 10)},
	}

	signURL := url.URL{
		Scheme:   "http",
		Host:     i.replicationGroupID,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequest(http.MethodGet, signURL.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (i *IAMAuthTokenRequest) sign(req *http.Request, credential aws.Credentials) (signedURI string, signedHeaders http.Header, err error) {
	s := signer.NewSigner()
	return s.PresignHTTP(i.ctx, credential, req, iam.EmptyBodySHA256, serviceName, i.region, time.Now())
}
