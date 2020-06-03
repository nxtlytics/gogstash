package filtergeoip2

import (
	"context"
	"net"

	geoip2 "github.com/oschwald/geoip2-golang"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "geoip2"

// ErrorTag tag added to event when process geoip2 failed
const ErrorTag = "gogstash_filter_geoip2_error"

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig

	DBPath  string `json:"db_path"`  // geoip2 db file path, default: GeoLite2-City.mmdb
	IPField string `json:"ip_field"` // IP field to get geoip info
	Key     string `json:"key"`      // geoip destination field name, default: geoip

	db *geoip2.Reader
}

// DefaultFilterConfig returns an FilterConfig struct with default values
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		FilterConfig: config.FilterConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		DBPath: "GeoLite2-City.mmdb",
		Key:    "geoip",
	}
}

// InitHandler initialize the filter plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeFilterConfig, error) {
	conf := DefaultFilterConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}

	conf.db, err = geoip2.Open(conf.DBPath)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) logevent.LogEvent {
	if event.Extra == nil {
		event.Extra = map[string]interface{}{}
	}

	ipstr := event.GetString(f.IPField)
	record, err := f.db.City(net.ParseIP(ipstr))
	if err != nil {
		config.Logger.Error(err)
		event.AddTag(ErrorTag)
		return event
	}
	if record == nil {
		event.AddTag(ErrorTag)
		return event
	}
	if record.Location.Latitude == 0 && record.Location.Longitude == 0 {
		event.AddTag(ErrorTag)
		return event
	}

	event.Extra[f.Key] = map[string]interface{}{
		"city": map[string]interface{}{
			"name": record.City.Names["en"],
		},
		"continent": map[string]interface{}{
			"code": record.Continent.Code,
			"name": record.Continent.Names["en"],
		},
		"country": map[string]interface{}{
			"code": record.Country.IsoCode,
			"name": record.Country.Names["en"],
		},
		"ip":        ipstr,
		"latitude":  record.Location.Latitude,
		"location":  []float64{record.Location.Longitude, record.Location.Latitude},
		"longitude": record.Location.Longitude,
		"timezone":  record.Location.TimeZone,
	}

	return event
}
