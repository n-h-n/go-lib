package env

import "os"

type Environment string

const (
	Local Environment = "LOCAL"
	Dev   Environment = "DEV"
	QA    Environment = "QA"
	Stg   Environment = "STG"
	Prod  Environment = "PROD"
)

func E() Environment {
	return Environment(os.Getenv("ENV"))
}

func FromString(s string) Environment {
	return Environment(s)
}

func IsValid(e Environment) bool {
	return e == Local || e == Dev || e == Prod
}
