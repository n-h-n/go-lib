package env

import "os"

type Environment string

const (
	Local Environment = "LOCAL"
	Dev   Environment = "DEV"
	Prod  Environment = "PROD"
)

func E() Environment {
	return Environment(os.Getenv("ENV"))
}
