package rds_postgres

// ClientOpt configures a Client in NewClient.
type ClientOpt func(*Client) error

func WithPort(port int) ClientOpt {
	return func(c *Client) error {
		c.port = port
		return nil
	}
}

func WithUser(user string) ClientOpt {
	return func(c *Client) error {
		c.user = user
		return nil
	}
}

func WithRegion(region string) ClientOpt {
	return func(c *Client) error {
		c.region = region
		return nil
	}
}

func WithVerbose(v bool) ClientOpt {
	return func(c *Client) error {
		c.verboseMode = v
		return nil
	}
}

func WithSSLModeVerifyFull() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "verify-full"
		return nil
	}
}

func WithSSLModeDisable() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "disable"
		return nil
	}
}

func WithSSLModeAllow() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "allow"
		return nil
	}
}

func WithSSLModePrefer() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "prefer"
		return nil
	}
}

func WithSSLModeRequire() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "require"
		return nil
	}
}

func WithSSLModeVerifyCA() ClientOpt {
	return func(c *Client) error {
		c.sslMode = "verify-ca"
		return nil
	}
}

func WithSSLCertFilePath(sslCertFilePath string) ClientOpt {
	return func(c *Client) error {
		c.sslCertFilePath = sslCertFilePath
		return nil
	}
}

func WithDBName(dbName string) ClientOpt {
	return func(c *Client) error {
		c.dbName = dbName
		return nil
	}
}

func WithPassword(password string) ClientOpt {
	return func(c *Client) error {
		c.password = password
		return nil
	}
}

// WithDialHost overrides the TCP host in the DSN (e.g. "127.0.0.1" for an SSM
// port-forward). IAM auth tokens still use the canonical host passed to NewClient.
func WithDialHost(host string) ClientOpt {
	return func(c *Client) error {
		c.dialHost = host
		return nil
	}
}

// WithDialPort overrides the TCP port in the DSN (e.g. local forward port).
// IAM auth tokens still use WithPort / the canonical RDS port.
func WithDialPort(port int) ClientOpt {
	return func(c *Client) error {
		c.dialPort = port
		return nil
	}
}
