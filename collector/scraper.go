package collector

import (
  "context"

  "github.com/prometheus/client_golang/prometheus"

)

// Scraper is minimal interface that let's you add new prometheus metrics to cloudera_exporter
type Scraper interface {
	// Name of the Scraper. Should be unique.
	Name() string

	// Help describes the role of the Scraper.
	Help() string

	// Version.
	Version() float64

	// Scrape collects data from cloudera_manager connection and sends it over channel as prometheus metric.
	Scrape(ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error
}

