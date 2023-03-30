/*
 *
 * title           :collector.go
 * description     :Collector definition
 * author		       :Raul Barroso and Alejandro Villegas
 * date            :05/10/2018
 *
 */
package collector




/* ======================================================================
 * Dependencies and libraries
 * ====================================================================== */
import (
  // Go Default libraries
  "context"

  // Go Prometheus libraries
  "github.com/prometheus/client_golang/prometheus"
)




/* ======================================================================
 * Constants
 * ====================================================================== */
const namespace = "kbdi"
const subsystem = "exporter"




/* ======================================================================
 * Exporter collects Cloudera Manager metrics. It implements prometheus.Collector.
 * ====================================================================== */
type Collector_connection_data struct {
  Host string
  Port string
  UseTls bool
  Api_version string
  User string
  Passwd string
}

type Collector struct {
	ctx      context.Context
	config   Collector_connection_data
	scrapers []Scraper
	metrics  Metrics
}




/* ======================================================================
* Functions
 * ====================================================================== */
// New returns a new Cloudera Manager exporter for the provided configs.
func New(ctx context.Context, config Collector_connection_data, metrics Metrics, scrapers []Scraper) *Collector {
	return &Collector{
		ctx:      ctx,
		config:   config,
		scrapers: scrapers,
		metrics:  metrics,
	}
}


// Describe implements prometheus.Collector.
func (c *Collector) Describe (ch chan<- *prometheus.Desc) {
	ch <- c.metrics.TotalScrapes.Desc()
	ch <- c.metrics.Error.Desc()
	c.metrics.ScrapeErrors.Describe(ch)
	ch <- c.metrics.CMUp.Desc()
}


// Collect implements prometheus.Collector.
func (c *Collector) Collect (ch chan<- prometheus.Metric) {
	c.scrape(c.ctx, ch)
	ch <- c.metrics.TotalScrapes
	ch <- c.metrics.Error
	c.metrics.ScrapeErrors.Collect(ch)
	ch <- c.metrics.CMUp
}
