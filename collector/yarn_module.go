package collector

import (
	"context"
	//"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric descriptors.
var (
       myNewYARNMetric  = prometheus.NewDesc(
                prometheus.BuildFQName(namespace, "subsystem", "metric_name"),
                "This is my metrics description.",
                []string{"label1","label2","label3"}, nil,)
    )

// ScrapeGlobalStatus collects from /clusters/hosts.
type ScrapeYARNMetrics struct{}

// Name of the Scraper. Should be unique.
func (ScrapeYARNMetrics) Name() string {
    return "YARN"
}

// Help describes the role of the Scraper.
func (ScrapeYARNMetrics) Help() string {
    return "Collect YARN Service Metrics"
}

// Version.
func (ScrapeYARNMetrics) Version() float64 {
    return 1.0
}

func (ScrapeYARNMetrics) Scrape(ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error {

    //user := config.User
    //passwd := config.Passwd
    //host := config.Host
    //port := config.Port
    //timeseries := config.Timeseries_api_version

    ////Here I describe my metric. This example below is for impala metric with tsquery.
    //urlStr := fmt.Sprintf("http://%s:%s/api/%s/timeseries?query=select+last(impala_query_admission_wait_rate)+where+entityName+rlike+\".*impala.*\"",host,port,timeseries)
    //jsonTimeseries, _ := make_query(ctx, urlStr, user, passwd)
    //number_metrics := gjson.Parse(string(jsonTimeseries)).Get("items.0.timeSeries.#").Int()
    //for counter_metrics := 0; counter_metrics < int(number_metrics) ; counter_metrics++{
    //    category := gjson.Parse(string(jsonTimeseries)).Get(fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.category", int(counter_metrics))).String()
    //    entityName := gjson.Parse(string(jsonTimeseries)).Get(fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.entityName", int(counter_metrics))).String()
    //    cluster := gjson.Parse(string(jsonTimeseries)).Get(fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.clusterDisplayName", int(counter_metrics))).String()
    //    value := gjson.Parse(string(jsonTimeseries)).Get(fmt.Sprintf("items.0.timeSeries.%d.data.0.value", int(counter_metrics))).Float()
    //    ch <- prometheus.MustNewConstMetric(myNewYARNMetric, prometheus.GaugeValue, value , category , entityName, cluster)
    //}

return nil

}


// check interface
var _ Scraper = ScrapeYARNMetrics{}
