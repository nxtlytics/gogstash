package outputkafka

import (
	"context"

	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
	"github.com/Shopify/sarama"
	"encoding/json"
	"fmt"
	"github.com/rcrowley/go-metrics"
	tags "github.com/arzh/go-metrics-tags"
	"github.com/tsaikd/gogstash/output/kafka/firehose"
)

// ModuleName is the name used in config file
const ModuleName = "kafka"

// ErrorTag tag added to event when process module failed
const ErrorTag = "gogstash_output_kafka_error"

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	ProducerConfig *sarama.Config
	Addresses []string
	producer sarama.AsyncProducer
}

// DefaultOutputConfig returns an OutputConfig struct with default values
func DefaultOutputConfig() OutputConfig {
	return OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		ProducerConfig: sarama.NewConfig(),
		Addresses: []string{"localhost:8080"},
	}
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	conf := DefaultOutputConfig()
	err := config.ReflectConfig(raw, &conf);
	fmt.Println("Kafka Config:", conf)
	if err != nil {
		return nil, err
	}
	conf.ProducerConfig.MetricRegistry = config.MetricRegistry;
	conf.producer, err = sarama.NewAsyncProducer(conf.Addresses, conf.ProducerConfig)
	if err != nil {
		return nil, err
	}

	go func() {
		c := conf.producer.Successes()
		for true {
			//fmt.Println("Waiting for a Success")
			e, open := <-c
			if !open {
				fmt.Println("Successes channel is closed")
				break
			}


			msg, err := e.Value.Encode()
			topic := ""
			if err == nil {

				topic = firehose.GetTopic(msg)
			}

			//fmt.Println("Success logging")
			//config.Logger.Info("Successfully sent msg to kafka:", e.Topic, "|", e.Value)
			name := tags.NameWithTags("kafka-producer-success", []string{"topic:" + topic})
			metric := config.MetricRegistry.Get(name)
			if metric == nil {
				meter := metrics.NewMeter();
				meter.Mark(1)
				config.MetricRegistry.Register(name, meter)
			} else {
				metric.(metrics.Meter).Mark(1)
			}
		}
	}()

	go func() {
		c := conf.producer.Errors()
		for true {
			//fmt.Println("Waiting for an Error")
			e, open := <-c
			if !open {
				fmt.Println("Errors channel is closed")

				break
			}

			msg, err := e.Msg.Value.Encode()
			topic := ""
			if err == nil {

				topic = firehose.GetTopic(msg)
			}

			fmt.Println("Error logging")
			config.Logger.Error(e)
			name := tags.NameWithTags("kafka-producer-failure", []string{"topic:" + topic})
			metric := config.MetricRegistry.Get(name)
			if metric == nil {
				meter := metrics.NewMeter();
				meter.Mark(1)
				config.MetricRegistry.Register(name, meter)
			} else {
				metric.(metrics.Meter).Mark(1)
			}
		}
	}()

	return &conf, nil
}

type JsonMapEncoder struct {
	b []byte
	err error
}

func NewJsonMapEncoder(m map[string]interface{}) JsonMapEncoder {

	// Building a topic is a bit complicated
	topic := "events."

	prefix := ""

	eventType, ok := m["event_type"].(string)
	if !ok {
		eventType = "unknown"
	}

	event, ok := m["event"].(string)
	if !ok {
		topic += prefix + eventType
	} else {
		topic += eventType + "." + prefix + event;
	}


	message, err := json.Marshal(m)
	if err != nil {
		message = []byte{}
	}

	buf := firehose.Encode(topic, message)

	return JsonMapEncoder{buf, err}
}

func (j JsonMapEncoder) Encode() ([]byte, error) {
	return j.b, j.err
}

func (j JsonMapEncoder) Length() int {
	return len(j.b)
}

// Output event
func (t *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	message := &sarama.ProducerMessage{Topic: "events.firehose", Value: NewJsonMapEncoder(event.Extra)}
	t.producer.Input() <- message
	return nil
}
