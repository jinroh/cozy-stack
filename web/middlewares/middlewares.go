package middlewares

import (
	"strings"

	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/labstack/echo"
)

// Compose can be used to compose a list of middlewares together with a main
// handler function. It returns a new handler that should be the composition of
// all the middlwares with the initial handler.
func Compose(handler echo.HandlerFunc, mws ...echo.MiddlewareFunc) echo.HandlerFunc {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

// SplitHost returns a splitted host domain taking into account the subdomains
// configuration mode used.
func SplitHost(host string) (instanceHost, appSlug, siblings string, ok bool) {
	var subDomain string
	var parentDomain string

	if mdn := config.GetConfig().MainDomainName; mdn != "" {
		if !strings.HasSuffix(host, mdn) {
			return
		}
		subDomain = strings.TrimSuffix(host, mdn)
		parentDomain = mdn[1:]
	} else {
		parts := strings.SplitN(host, ".", 2)
		if len(parts) != 2 {
			return host, "", "", true
		}
		subDomain = parts[0]
		parentDomain = parts[1]
	}

	if config.GetConfig().Subdomains == config.NestedSubdomains {
		return parentDomain, subDomain, "*." + parentDomain, true
	}

	subs := strings.SplitN(subDomain, "-", 2)
	if len(subs) == 2 {
		return subs[0] + "." + parentDomain, subs[1], "*." + parentDomain, true
	}
	return host, "", "", true
}
