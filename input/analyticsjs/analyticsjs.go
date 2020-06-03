package analyticsjs

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"math/rand"
	"time"
	"context"

	"github.com/sirupsen/logrus"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
	"github.com/satori/go.uuid"
	"github.com/rcrowley/go-metrics"
	tags "github.com/arzh/go-metrics-tags"

)

// ModuleName is the name used in config file
const (
	ModuleName = "analyticsjs"

	invalidJSONMsgf = "Invalid JSON received on HTTP listener. Decoder error: %+v"
	failureBodyParsingJson = "{\"error\": \"Failed to parse the body\"}"
	failureAddContextJson = "{\"error\": \"Failed to add context\"}"
)

var (
	// Defaults, should change these to config entries...
	CorsAllowed = ""
	EventSampling = make(map[string]int)
	CookieDomain = ""
)

// SamplingFilterError - Means that the message was filtered out by the sampling rate
type SamplingFilterError struct {
}

func (err SamplingFilterError) Error() string {
	return "Filtered Based on sampling rate"
}

func init() {
	corsEV := os.Getenv("CORS_ALLOWED")
	if corsEV != "" {
		CorsAllowed = corsEV
	}

	samplesEV := os.Getenv("EVENT_SAMPLING")
	if samplesEV != "" {
		for _, s := range strings.Split(samplesEV, ",") {
			duple := strings.Split(s, ":")
			event := duple[0]
			rate, err := strconv.Atoi(duple[1])

			if err != nil {
				fmt.Println("FAILED TO PARSE SAMPLE:", s, err)
				continue
			}

			EventSampling[event] = rate
		}
	}

	cookieDomainEV := os.Getenv("COOKIE_DOMAIN")
	if cookieDomainEV != "" {
		CookieDomain = cookieDomainEV
	}
}

// const invalidAccessTokenMsg = "Invalid access token. Access denied."
func logInputChannel(msgChan chan<- logevent.LogEvent, metricsRegistry metrics.Registry) {
	ticker := time.NewTicker(time.Millisecond * 500)
	gauge := metrics.NewGauge()
	metricsRegistry.Register("kafka-input-queue", gauge)
	for _ = range ticker.C {
		nq := int64(len(msgChan))
		gauge.Update(nq)
	}
}

type ProjectInfo struct {
	id string
	channel string
}

func getProjectInfo(writeId string) ProjectInfo {
	//Eventually we will have a map[string]ProjectInfo to pull from the writeId in the request
	return ProjectInfo{"placeholder", "client"}
}

func parseBodyMapJson(meta map[string]interface{}, rw http.ResponseWriter, req *http.Request) error {

	dec := json.NewDecoder(req.Body)

	// attempt to decode post body, if it fails, log it.
	if err := dec.Decode(&meta); err != nil {
		config.Logger.Warnf(invalidJSONMsgf, err)
		config.Logger.Debugf("Invalid JSON: '%s'", req.Body)
		tags.TagMetric(config.MetricRegistry, "invalid-json-parsed", []string{req.URL.Path}, metrics.NewMeter()).(metrics.Meter).Mark(1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf(invalidJSONMsgf, err)))
		return err
	}

	return nil
}

func addContext(meta map[string]interface{}, rw http.ResponseWriter, req *http.Request) error {

	var context map[string]interface{}
	var ok bool

	ci, ok := meta["context"]
	if ok {
		context, ok = ci.(map[string]interface{})
		if !ok {
			return errors.Errorf("Had a context that could not be converted to a map: %s", ci)
		}
	} else {
		context = make(map[string]interface{})
	}

	clientIp := req.Header.Get("True-Client-IP")
	context["ip"] = clientIp

	meta["context"] = context

	return nil
}

func setCorsWithPreflight(meta map[string]interface{}, rw http.ResponseWriter, req *http.Request) error {
	if origin := req.Header.Get("Origin"); origin != "" {
		rw.Header().Set("Access-Control-Allow-Origin", origin)
		rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		rw.Header().Set("Access-Control-Allow-Credentials", "true")
		rw.Header().Set("Access-Control-Allow-Headers", "Content-type, Access-Control-Allow-Credentials, Access-Control-Allow-Headers, Access-Control-Allow-Methods, Access-Control-Allow-Origin, Authorization, Cookie")
	} else {
		rw.Header().Set("Access-Control-Allow-Origin", CorsAllowed)
		rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		rw.Header().Set("Access-Control-Allow-Credentials", "true")
		rw.Header().Set("Access-Control-Allow-Headers", "Content-type, Access-Control-Allow-Credentials, Access-Control-Allow-Headers, Access-Control-Allow-Methods, Access-Control-Allow-Origin, Authorization, Cookie")
	}

	if req.Method == http.MethodOptions {
		rw.WriteHeader(http.StatusOK)
		return errors.New("Options Preflight");
	}

	return nil
}

func inSampleRate(event string) bool {
	rate, hasEvent := EventSampling[event]

	if hasEvent{
		if rate > rand.Intn(100) {
			return true
		}

		return false
	}

	return true
}

func (i *InputConfig) healthHandler(rw http.ResponseWriter, req *http.Request) {
	i.logger.Debug("health check")
	rw.Write([]byte("{\"status\": \"Healthy\"}"))
}

// InputConfig holds the configuration json fields and internal objects
type InputConfig struct {
	config.InputConfig
	Address       string   `json:"address"`        // host:port to listen on
	//RequireHeader []string `json:"require_header"` // Require this header to be present to accept the POST ("X-Access-Token: Potato")
	msgChan       chan<- logevent.LogEvent
	logger        *logrus.Logger
}

func DefaultInputConfig() InputConfig {
	return InputConfig{
		InputConfig: config.InputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address:       "0.0.0.0:8080",
		//RequireHeader: []string{},
	}
}

// InitHandler initialize the input plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeInputConfig, error) {
	conf := DefaultInputConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}

// Start wraps the actual function starting the plugin
func (i *InputConfig) Start(ctx context.Context, msgChan chan<- logevent.LogEvent) (err error) {
	i.logger = config.Logger
	i.msgChan = msgChan

	go logInputChannel(msgChan, config.MetricRegistry)

	// returns {"success": true}
	http.HandleFunc("/p", i.pageHandler)
	http.HandleFunc("/i", i.identifyHandler)
	http.HandleFunc("/g", i.groupHandler)
	http.HandleFunc("/t", i.trackHandler)
	http.HandleFunc("/a", i.aliasHandler)

	http.HandleFunc("/v1/batch", i.batchHandler)

	http.HandleFunc("/health", i.healthHandler)
	http.HandleFunc("/v1/id/", idHandler)

	go func() {
		i.logger.Infof("accepting requests to %s", i.Address)
		if err = http.ListenAndServe(i.Address, nil); err != nil {
			i.logger.Fatal(err)
		}
	}()
	return nil
}

func idHandler(rw http.ResponseWriter, req *http.Request) {
	if err := setCorsWithPreflight(nil, rw, req); err != nil {
		return
	}

	idVal := ""

	seg_xid, err := req.Cookie("seg_xid")
	if err != nil && err == http.ErrNoCookie {
		idUuid, err := uuid.NewV4()
		if err == nil {
			idVal = idUuid.String()
		}

		xidCookie := http.Cookie{Name: "seg_xid", Value: idVal, Path: "/", Domain: CookieDomain, Expires: time.Now().Add(time.Hour * 8760)}
		http.SetCookie(rw, &xidCookie)
	} else {
		idVal = seg_xid.Value
	}

	fmt.Fprintf(rw,"{\"id\":\"%s\"}", idVal)
}

func (i *InputConfig) pageHandler(rw http.ResponseWriter, req *http.Request) {
	if err := setCorsWithPreflight(nil, rw, req); err != nil {
		return;
	}
	i.logger.Debugf("Received request")

	jsonMsg := make(map[string]interface{})
	if err := parseBodyMapJson(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureBodyParsingJson))
		return
	}
	if err := addContext(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureAddContextJson))
		return
	}

	projectInfo := getProjectInfo(jsonMsg["writeKey"].(string))

	err := i.logEvent(jsonMsg, projectInfo)
	if err != nil {
		if _, ok := err.(SamplingFilterError); ok {
			rw.WriteHeader(http.StatusNoContent);
		} else {
			rw.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	rw.WriteHeader(http.StatusAccepted)

}

func (i *InputConfig) batchHandler(rw http.ResponseWriter, req *http.Request) {
	if err := setCorsWithPreflight(nil, rw, req); err != nil {
		return;
	}

	writeKey, _, ok := req.BasicAuth()
	if !ok {
		writeKey = ""
	}

	projectInfo := getProjectInfo(writeKey)

	jsonMsg := make(map[string]interface{})
	if err := parseBodyMapJson(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureBodyParsingJson))
		return
	}
	if err := addContext(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureAddContextJson))
		return
	}

	ar, ok := jsonMsg["batch"].([]interface{})
	if !ok {
		i.logger.Error("Could not parse the batch to array:", jsonMsg["batch"])
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureAddContextJson))
		return
	}

	for _, v := range ar {
		m, ok := v.(map[string]interface{})
		if !ok {
			i.logger.Error("BATCH: Failed to map the entry to a map:", v)
			continue //For now I want to skip the events that are failing
		}
		if err := addContext(m, rw, req); err != nil {
			i.logger.Error("BATCH: Failed to add context:", v)
			continue //For now I want to skip the events that are failing
		}
		m["writeKey"] = interface{}(writeKey)
		i.logEvent(m, projectInfo)
	}

}

const JS_TIME_LAYOUT = "2006-01-02T15:04:05.999Z"

func modDataMap(m map[string]interface{}, projectInfo ProjectInfo) {
	m["receivedAt"] = time.Now().Format(JS_TIME_LAYOUT)
	m["version"] = 2
	m["projectId"] = projectInfo.id
	m["channel"] = projectInfo.channel

	m["originalTimestamp"] = m["timestamp"]
	m["timestamp"] = time.Now().Format(JS_TIME_LAYOUT)

	m["event_type"] = m["type"]
	delete(m, "type")
}

func mark1(name, event string) {
	tag := tags.NameWithTags(name, []string{"event:" + event})

	metric := config.MetricRegistry.Get(tag)
	if metric == nil {
		meter := metrics.NewMeter()
		meter.Mark(1)
		config.MetricRegistry.Register(tag, meter)
	} else {
		metric.(metrics.Meter).Mark(1)
	}
}

func (i *InputConfig) logEvent(m map[string]interface{}, projectInfo ProjectInfo) error {
	eventName, hasEvent := m["event"]

	if hasEvent {
		event_str, isString := eventName.(string)

		if isString {

			mark1("ajs-log-event-total", event_str)

			if !inSampleRate(event_str) {
				return SamplingFilterError{}
			}

			mark1("ajs-log-event", event_str)
		}
	}

	modDataMap(m, projectInfo)
	// send the event as it came to us
	i.msgChan <- logevent.LogEvent{
		Timestamp: time.Now(),
		Extra:     m,
	}

	return nil
}

func (i *InputConfig) identifyHandler(rw http.ResponseWriter, req *http.Request) {
	if err := setCorsWithPreflight(nil, rw, req); err != nil {
		return;
	}

	jsonMsg := make(map[string]interface{})


	if err := parseBodyMapJson(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureBodyParsingJson))
		return
	}
	if err := addContext(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureAddContextJson))
		return
	}


	// Set the cookies...
	_, err := req.Cookie("ajs_anonymous_id")
	if err != nil && err == http.ErrNoCookie {
		idVal := ""
		if jsonMsg["anonymousId"] == nil {
			idUuid, err := uuid.NewV4()
			if err == nil {
				idVal = idUuid.String()
				jsonMsg["anonymousId"] = idVal
			}
		} else {
			idVal = jsonMsg["anonymousId"].(string)
		}

		ajsAnon := http.Cookie{Name: "ajs_anonymous_id", Value: idVal, Path: "/", Domain: CookieDomain, Expires: time.Now().Add(time.Hour * 8760)}
		http.SetCookie(rw, &ajsAnon)

		ajsUser := http.Cookie{Name: "ajs_user_id", Value: jsonMsg["userId"].(string), Path: "/", Domain: CookieDomain, Expires: time.Now().Add(time.Hour * 8760)}
		http.SetCookie(rw, &ajsUser)
	}

	projectInfo := getProjectInfo(jsonMsg["writeKey"].(string))
	err = i.logEvent(jsonMsg, projectInfo)

	if err != nil {
		if _, ok := err.(SamplingFilterError); ok {
			rw.WriteHeader(http.StatusNoContent)
		} else {
			rw.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	rw.WriteHeader(http.StatusAccepted)
}

func (i *InputConfig) groupHandler(rw http.ResponseWriter, req *http.Request) {
 // We don't use this one
 rw.WriteHeader(http.StatusNotImplemented)
}

func (i *InputConfig) trackHandler(rw http.ResponseWriter, req *http.Request) {
	if err := setCorsWithPreflight(nil, rw, req); err != nil {
		return
	}
	i.logger.Debugf("Received request")

	jsonMsg := make(map[string]interface{})

	// attempt to decode post body, if it fails, log it.
	if err := parseBodyMapJson(jsonMsg, rw, req); err != nil {
		return
	}
	if err := addContext(jsonMsg, rw, req); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(failureAddContextJson))
		return
	}
	projectInfo := getProjectInfo(jsonMsg["writeKey"].(string))
	err := i.logEvent(jsonMsg, projectInfo)

	if err != nil {
		if _, ok := err.(SamplingFilterError); ok {
			rw.WriteHeader(http.StatusNoContent)
		} else {
			rw.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	rw.WriteHeader(http.StatusAccepted)

}

func (i *InputConfig) aliasHandler(rw http.ResponseWriter, req *http.Request) {
 // We don't use this one
 rw.WriteHeader(http.StatusNotImplemented)
}


