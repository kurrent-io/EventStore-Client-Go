package samples

import (
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
)

func UserCertificates() {
	// region client-with-user-certificates
	settings, err := kurrentdb.ParseConnectionString("esdb://admin:changeit@{endpoint}?tls=true&userCertFile={pathToCaFile}&userKeyFile={pathToKeyFile}")

	if err != nil {
		panic(err)
	}

	db, err := kurrentdb.NewClient(settings)
	// endregion client-with-user-certificates

	if err != nil {
		panic(err)
	}

	db.Close()
}
