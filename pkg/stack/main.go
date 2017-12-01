package stack

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cozy/checkup"
	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/instance"
	"github.com/cozy/cozy-stack/pkg/jobs"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/scheduler"
	"github.com/cozy/cozy-stack/pkg/sessions"
	"github.com/cozy/cozy-stack/pkg/utils"

	"github.com/google/gops/agent"
	"github.com/sirupsen/logrus"
)

var log = logger.WithNamespace("stack")

type gopAgent struct{}

func (g gopAgent) Shutdown(ctx context.Context) error {
	fmt.Print("  shutting down gops...")
	agent.Close()
	fmt.Println("ok.")
	return nil
}

// Start is used to initialize all the
func Start() (broker jobs.Broker, schder scheduler.Scheduler, processes utils.Shutdowner, err error) {
	if config.IsDevRelease() {
		fmt.Println(`                           !! DEVELOPMENT RELEASE !!
You are running a development release which may deactivate some very important
security features. Please do not use this binary as your production server.
`)
	}

	err = agent.Listen(agent.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error on gops agent: %s\n", err)
	}

	// Check that we can properly reach CouchDB.
	u := config.CouchURLCopy()
	u.User = config.GetConfig().CouchDB.Auth
	attempts := 8
	attemptsSpacing := 1 * time.Second
	for i := 0; i < attempts; i++ {
		var db checkup.Result
		db, err = checkup.HTTPChecker{
			URL:         u.String(),
			MustContain: `"version":"2`,
		}.Check()
		if err != nil {
			err = fmt.Errorf("Could not reach Couchdb 2.0 database: %s", err.Error())
		} else if db.Status() == checkup.Down {
			err = fmt.Errorf("Could not reach Couchdb 2.0 database")
		} else if db.Status() != checkup.Healthy {
			log.Warnf("CouchDB does not seem to be in a healthy state, " +
				"the cozy-stack will be starting anyway")
			break
		}
		if err == nil {
			break
		}
		if i < attempts-1 {
			logrus.Warnf("%s, retrying in %v", err, attemptsSpacing)
			time.Sleep(attemptsSpacing)
		}
	}
	if err != nil {
		return
	}

	// Init the main global connection to the swift server
	fsURL := config.FsURL()
	if fsURL.Scheme == config.SchemeSwift {
		if err = config.InitSwiftConnection(fsURL); err != nil {
			return
		}
	}

	// Start update cron for auto-updates
	cronUpdates, err := instance.StartUpdateCron()
	if err != nil {
		return
	}

	jobsConfig := config.GetConfig().Jobs
	nbWorkers := jobsConfig.Workers
	if cli := jobsConfig.Redis.Client(); cli != nil {
		broker = jobs.NewRedisBroker(nbWorkers, cli)
		schder = scheduler.NewRedisScheduler(cli)
	} else {
		broker = jobs.NewMemBroker(nbWorkers)
		schder = scheduler.NewMemScheduler()
	}
	if err = broker.Start(jobs.GetWorkersList()); err != nil {
		return
	}
	if err = schder.Start(broker); err != nil {
		return
	}

	sessionSweeper := sessions.SweepLoginRegistrations()

	// Global shutdowner that composes all the running processes of the stack
	processes = utils.NewGroupShutdown(
		broker,
		schder,
		cronUpdates,
		sessionSweeper,
		gopAgent{},
	)
	return
}
