package DatabaseCredentials

import (
	"log"
	"os"
)

type DatabaseCredentials struct {
	ServerAddr string
	User       string
	Password   string
	Port       string
	Database   string
}

func GetDatabaseCredentialsFromEnv() DatabaseCredentials {
	lookupOrFatal := func(env string) string {
		val, success := os.LookupEnv(env)
		if !success {
			log.Fatalf("Environment variable %s is not set.", env)
		}
		return val
	}
	var cred DatabaseCredentials
	cred.ServerAddr = lookupOrFatal("SQL_SERVER_ADDR")
	cred.User = lookupOrFatal("SQL_SERVER_USER")
	cred.Password = lookupOrFatal("SQL_SERVER_PASSWORD")
	cred.Port = lookupOrFatal("SQL_SERVER_PORT")
	cred.Database = lookupOrFatal("SQL_SERVER_DB")

	return cred
}
