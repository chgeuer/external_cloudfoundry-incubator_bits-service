package statsd

import (
	"time"

	"github.com/tecnickcom/statsd"
)

type MetricsService struct {
	statsdClient *statsd.Client
	prefix       string
}

func NewMetricsService() *MetricsService {
	statsdClient, e := statsd.New() // Connect to the UDP port 8125 by default.
	if e != nil {
		panic(e)
	}
	return &MetricsService{statsdClient: statsdClient, prefix: "bits."}
}

func (service *MetricsService) SendTimingMetric(name string, duration time.Duration) {
	service.statsdClient.Timing(service.prefix+name, duration.Seconds()*1000)
}
func (service *MetricsService) SendGaugeMetric(name string, value int64) {
	service.statsdClient.Gauge(service.prefix+name, value)
}