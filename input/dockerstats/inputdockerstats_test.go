package inputdockerstats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tsaikd/gogstash/config"
)

var (
	logger = config.Logger
)

func init() {
	logger.Level = logrus.DebugLevel
	config.RegistInputHandler(ModuleName, InitHandler)
}

func Test_input_dockerstats_module(t *testing.T) {
	assert := assert.New(t)
	assert.NotNil(assert)
	require := require.New(t)
	require.NotNil(require)

	ctx := context.Background()
	conf, err := config.LoadFromYAML([]byte(strings.TrimSpace(`
debugch: true
input:
  - type: dockerstats
    dockerurl: "unix:///var/run/docker.sock"
    stat_interval: 3
	`)))
	require.NoError(err)
	err = conf.Start(ctx)
	if err != nil {
		require.True(ErrorPingFailed.In(err))
		t.Skip("skip test input dockerstats module")
	}

	time.Sleep(500 * time.Millisecond)
	if event, err := conf.TestGetOutputEvent(100 * time.Millisecond); assert.NoError(err) {
		t.Log(event)
	}
}
