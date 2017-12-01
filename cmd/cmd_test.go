package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cozy/checkup"
	"github.com/cozy/cozy-stack/client"
	"github.com/cozy/cozy-stack/client/request"
	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/instance"
	"github.com/cozy/cozy-stack/pkg/permissions"
	"github.com/cozy/cozy-stack/web"
	"github.com/labstack/echo"
	"github.com/stretchr/testify/assert"
)

var testInstance *instance.Instance
var testClient *client.Client

func TestMain(m *testing.M) {
	config.UseTestFile()

	db, err := checkup.HTTPChecker{URL: config.CouchURL("/")}.Check()
	if err != nil || db.Status() != checkup.Healthy {
		fmt.Println("This test need couchdb to run.")
		os.Exit(1)
	}

	tempdir, err := ioutil.TempDir("", "cozy-stack")
	if err != nil {
		fmt.Println("Could not create temporary directory.")
		os.Exit(1)
	}

	config.GetConfig().Fs.URL = &url.URL{
		Scheme: "file",
		Host:   "localhost",
		Path:   tempdir,
	}
	server := echo.New()
	err = web.SetupRoutes(server)
	if err != nil {
		fmt.Println("Could not start server", err)
		os.Exit(1)
	}

	ts := httptest.NewServer(server)
	u, _ := url.Parse(ts.URL)
	domain := strings.Replace(u.Host, "127.0.0.1", "localhost", -1)

	instance.Destroy(domain)
	testInstance, err = instance.Create(&instance.Options{
		Domain: domain,
		Locale: "en",
	})
	if err != nil {
		fmt.Println("Could not create test instance.", err)
		os.Exit(1)
	}

	token, err := testInstance.MakeJWT(permissions.CLIAudience, "CLI", consts.Files, "", time.Now())
	if err != nil {
		fmt.Println("Could not get test instance token.", err)
		os.Exit(1)
	}

	testClient = &client.Client{
		Domain:     domain,
		Authorizer: &request.BearerAuthorizer{Token: token},
	}

	res := m.Run()
	instance.Destroy("test-files")
	os.RemoveAll(tempdir)
	ts.Close()

	os.Exit(res)
}

func TestExecCommand(t *testing.T) {
	buf := new(bytes.Buffer)
	err := execCommand(testClient, "mkdir /hello-test", buf)
	assert.NoError(t, err)

	buf = new(bytes.Buffer)
	err = execCommand(testClient, "ls /", buf)
	assert.NoError(t, err)
	assert.True(t, bytes.Contains(buf.Bytes(), []byte("hello-test")))
}
