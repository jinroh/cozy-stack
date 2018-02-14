package config

import (
	"net/url"
	"strconv"

	minio "github.com/minio/minio-go"
)

var minioClient *minio.Client

// InitMinioConnection initialize the global minio handler connection. This is
// not a thread-safe method.
func InitMinioConnection(minioURL *url.URL) error {
	q := minioURL.Query()
	endpoint := q.Get("Endpoint")
	accessKeyID := q.Get("AccessKeyID")
	secretAccessKey := q.Get("SecretAccessKey")
	secure, _ := strconv.ParseBool(q.Get("Secure"))

	var err error
	minioClient, err = minio.New(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		log.Errorf("[minio] Could not create connection with endpoint %s (secure=%t)",
			endpoint, secure)
		return err
	}

	appName := q.Get("AppName")
	appVersion := q.Get("AppVersion")
	if appName != "" || appVersion != "" {
		minioClient.SetAppInfo(appName, appVersion)
	}

	return nil
}

// GetMinioConnection returns a minio.Client pointer created from the
// configuration.
func GetMinioConnection() *minio.Client {
	if minioClient == nil {
		panic("Called GetMinioConnection() before InitMinioConnection()")
	}
	return minioClient
}
