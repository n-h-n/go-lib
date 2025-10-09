package rds_postgres

type clientOpt func(*Client) error

func WithPort(port int) clientOpt {
	return func(c *Client) error {
		c.port = port
		return nil
	}
}

func WithUser(user string) clientOpt {
	return func(c *Client) error {
		c.user = user
		return nil
	}
}

func WithRegion(region string) clientOpt {
	return func(c *Client) error {
		c.region = region
		return nil
	}
}

func WithVerbose(v bool) clientOpt {
	return func(c *Client) error {
		c.verboseMode = v
		return nil
	}
}

func WithSSLModeVerifyFull() clientOpt {
	return func(c *Client) error {
		c.sslMode = "verify-full"
		return nil
	}
}

func WithSSLModeDisable() clientOpt {
	return func(c *Client) error {
		c.sslMode = "disable"
		return nil
	}
}

func WithSSLModeAllow() clientOpt {
	return func(c *Client) error {
		c.sslMode = "allow"
		return nil
	}
}

func WithSSLModePrefer() clientOpt {
	return func(c *Client) error {
		c.sslMode = "prefer"
		return nil
	}
}

func WithSSLModeRequire() clientOpt {
	return func(c *Client) error {
		c.sslMode = "require"
		return nil
	}
}

func WithSSLModeVerifyCA() clientOpt {
	return func(c *Client) error {
		c.sslMode = "verify-ca"
		return nil
	}
}

func WithSSLCertFilePath(sslCertFilePath string) clientOpt {
	return func(c *Client) error {
		c.sslCertFilePath = sslCertFilePath
		return nil
	}
}

func WithDBName(dbName string) clientOpt {
	return func(c *Client) error {
		c.dbName = dbName
		return nil
	}
}
