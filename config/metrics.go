package config

import (
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/arzh/go-metrics-datadog-tags"
	"os"
)



var (
	MetricRegistry metrics.Registry
)

func envWithDefault(key string, def string) string {
	item, found := os.LookupEnv("STATSD_URL")
	if !found {
		item = def
	}

	return item
}

func init() {
	MetricRegistry = metrics.NewRegistry()


	statsdUrl := envWithDefault("STATSD_URL", "127.0.0.1")
	env := envWithDefault("ENV", "dev")

	Logger.Println("StatsD url:", statsdUrl)

	reporter, err := datadog.NewReporter(
		MetricRegistry,  		// Metrics registry, or nil for default
		statsdUrl + ":8125",    // DogStatsD UDP address
		"gogstash.",	// Namespace
		time.Second*5,   		// Update interval
	)

	// global tags to help track multiple instances
	reporter.Tags = []string{
		"environment:" + env,
		}

	if err != nil {
		Logger.Fatal(err)
	}

	go reporter.Flush()

}
