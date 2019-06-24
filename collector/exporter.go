/*
 *
 * title           :collector/exporter_metrics_module.go
 * description     :Submodule for the Exporter metrics
 * author		       :Alejandro Villegas
 * date            :2019/03/08
 * version         :1.0
 *
 */
package collector




/* ======================================================================
 * Dependencies and libraries
 * ====================================================================== */
import (
  // Go Default libraries
  "context"
  "time"
  "sync"

  // Own libraries
  log "keedio/cloudera_exporter/logger"


  // Go Prometheus libraries
  "github.com/prometheus/client_golang/prometheus"
)




/* ======================================================================
 * Data Structs
 * ====================================================================== */
type Metrics struct {
	TotalScrapes  prometheus.Counter
	ScrapeErrors  *prometheus.CounterVec
	Error         prometheus.Gauge
	CMUp          prometheus.Gauge
}




/* ======================================================================
 * Global variables
 * ====================================================================== */



/* ======================================================================
 * Functions
 * ====================================================================== */
func NewMetrics() Metrics {
	return Metrics {
		TotalScrapes: prometheus.NewCounter(prometheus.CounterOpts {
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrapes_total",
			Help:      "Total number of times Cloudera Manager was scraped for metrics.",
		}),

		Error: prometheus.NewGauge(prometheus.GaugeOpts {
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Cloudera Manager resulted in an error (1 for error, 0 for success).",
		}),

		ScrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts {
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occurred scraping a Cloudera Manager.",
		}, []string{"collector"}),

		CMUp: prometheus.NewGauge(prometheus.GaugeOpts {
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "up",
			Help:      "Whether the Cloudera Manager server is up(1).",
		}),
	}
}

var scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "collector_duration_seconds"),
		"Collector time duration.",
		[]string{"collector"},
    nil,
	)



func (c *Collector) scrape (ctx context.Context, ch chan<- prometheus.Metric) {
	c.metrics.TotalScrapes.Inc()

	var wg sync.WaitGroup
	defer wg.Wait()
	for _, scraper := range c.scrapers {

		wg.Add(1)
		go func(scraper Scraper) {
			defer wg.Done()
			label := scraper.Name()
			scrapeTime := time.Now()
			if err := scraper.Scrape(ctx, &c.config, ch); err != nil {
				log.Err_msg("Error scraping for " + label + ":", err)
				c.metrics.ScrapeErrors.WithLabelValues(label).Inc()
				c.metrics.CMUp.Set(0)
				c.metrics.Error.Set(1)
			}
			c.metrics.CMUp.Set(1)
			ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, time.Since(scrapeTime).Seconds(), label)
		} (scraper)
	}

}
