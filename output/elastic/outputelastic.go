package outputelastic

import (
	"context"
	"strings"
	"time"

	"github.com/tsaikd/KDGoLib/errutil"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
	elastic "gopkg.in/olivere/elastic.v5"
)

// ModuleName is the name used in config file
const ModuleName = "elastic"

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	URL          string `json:"url"`           // elastic API entrypoint
	Index        string `json:"index"`         // index name to log
	DocumentType string `json:"document_type"` // type name to log
	DocumentID   string `json:"document_id"`   // id to log, used if you want to control id format

	Sniff bool `json:"sniff"` // find all nodes of your cluster, https://github.com/olivere/elastic/wiki/Sniffing

	// BulkActions specifies when to flush based on the number of actions
	// currently added. Defaults to 1000 and can be set to -1 to be disabled.
	BulkActions int `json:"bulk_actions,omitempty"`

	// BulkSize specifies when to flush based on the size (in bytes) of the actions
	// currently added. Defaults to 5 MB and can be set to -1 to be disabled.
	BulkSize int `json:"bulk_size,omitempty"`

	// BulkFlushInterval specifies when to flush at the end of the given interval.
	// Defaults to 30 seconds. If you want the bulk processor to
	// operate completely asynchronously, set both BulkActions and BulkSize to
	// -1 and set the FlushInterval to a meaningful interval.
	BulkFlushInterval time.Duration `json:"bulk_flush_interval"`

	client    *elastic.Client        // elastic client instance
	processor *elastic.BulkProcessor // elastic bulk processor
}

// DefaultOutputConfig returns an OutputConfig struct with default values
func DefaultOutputConfig() OutputConfig {
	return OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		BulkActions:       1000,    // 1000 actions
		BulkSize:          5 << 20, // 5 MB
		BulkFlushInterval: 30 * time.Second,
	}
}

// errors
var (
	ErrorCreateClientFailed1 = errutil.NewFactory("create elastic client failed: %q")
)

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	conf := DefaultOutputConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}

	if conf.client, err = elastic.NewClient(
		elastic.SetURL(conf.URL),
		elastic.SetSniff(conf.Sniff),
	); err != nil {
		return nil, ErrorCreateClientFailed1.New(err, conf.URL)
	}

	conf.processor, err = conf.client.BulkProcessor().
		Name("gogstash-output-elastic").
		BulkActions(conf.BulkActions).
		BulkSize(conf.BulkSize).
		FlushInterval(conf.BulkFlushInterval).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

// Output event
func (t *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	index := event.Format(t.Index)
	// elastic index name should be lowercase
	index = strings.ToLower(index)
	doctype := event.Format(t.DocumentType)
	id := event.Format(t.DocumentID)

	indexRequest := elastic.NewBulkIndexRequest().
		Index(index).
		Type(doctype).
		Id(id).
		Doc(event)
	t.processor.Add(indexRequest)

	return
}
