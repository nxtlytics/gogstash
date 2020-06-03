package filterdate

import (
	"context"
	"time"

	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "date"

// ErrorTag tag added to event when process module failed
const ErrorTag = "gogstash_filter_date_error"

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig

	Format string `json:"format"` // date parse format
	Source string `json:"source"` // source message field name
}

// DefaultFilterConfig returns an FilterConfig struct with default values
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		FilterConfig: config.FilterConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Format: time.RFC3339Nano,
		Source: "message",
	}
}

// InitHandler initialize the filter plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeFilterConfig, error) {
	conf := DefaultFilterConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) logevent.LogEvent {
	if event.Extra == nil {
		event.Extra = map[string]interface{}{}
	}

	timestamp, err := time.Parse(f.Format, event.GetString(f.Source))
	if err != nil {
		event.AddTag(ErrorTag)
		config.Logger.Error(err)
		return event
	}

	event.Timestamp = timestamp.UTC()
	return event
}
