package cache

import (
	"context"
	"fmt"

	"github.com/cozy/cozy-stack/pkg/utils"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/jinroh/immcache"
)

var caches []immcache.Immutable

// Cleaner purges all registered caches content on shutdown.
func Cleaner() utils.Shutdowner {
	return cleaner{}
}

type cleaner struct{}

func (c cleaner) Shutdown(ctx context.Context) error {
	var errm error
	fmt.Print("  shutting down caches...")
	for _, c := range caches {
		if err := c.PurgeAndClose(); err != nil {
			errm = multierror.Append(errm, err)
		}
	}
	if errm != nil {
		fmt.Printf("failed: %s\n", errm)
	} else {
		fmt.Println("ok.")
	}
	caches = caches[:0]
	return errm
}

// Register records the given cache to allow a purge on shutdown.
func Register(c immcache.Immutable) immcache.Immutable {
	caches = append(caches, c)
	return c
}
