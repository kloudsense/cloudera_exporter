/*
 *
 * title           :json_cloudera_api_timeseries_queries_parser.go
 * description     :File with specific functions to parse JSONs files
 * author		       :Alejandro Villegas Lopez (avillegas@keedio.com)
 * date            :2018/10/05
 * version         :0.1
 * notes           :Submodule 
 *
 */
package json_parser

/* 
 * Dependencies
 */
import (
  // Go Default libraries
  "fmt"
  "strconv"
  "errors"

  // Go JSON parsing libraries
  "github.com/tidwall/gjson"
)

// Base string to the Cloudera URL TimeSeries Query API
const TIMESERIES_API_BASE_URL="http://%s:%s/api/%s/timeseries?%s"

// Compose the URL connection to the Cloudera API TimeSeries Query
func Build_timeseries_api_query_url(host string, port string, timeseries_version string, query string) string {
  return fmt.Sprintf(TIMESERIES_API_BASE_URL, host, port, timeseries_version, query)
}

// Return the host_id metadata parameter from a TimeSeries Query
func Get_timeseries_query_host_id(json_timeseries gjson.Result, serie_index int) string {
  return Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.hostId", serie_index))
}

// Return the entityName metadata parameter from a TimeSeries Query
func Get_timeseries_query_entity_name(json_timeseries gjson.Result, serie_index int) string {
  return Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.entityName", serie_index))
}

// Return the host_name metadata parameter from a TimeSeries Query
func Get_timeseries_query_host_name(json_timeseries gjson.Result, serie_index int) string {
  return Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.hostname", serie_index))
}

// Return the cluster metadata parameter from a TimeSeries Query
func Get_timeseries_query_cluster_display_name(json_timeseries gjson.Result, serie_index int) string {
  return Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.clusterDisplayName", serie_index))
}

// Return the cluster metadata parameter from a TimeSeries Query
func Get_timeseries_query_cluster(json_timeseries gjson.Result, serie_index int) string {
  return Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.metadata.attributes.clusterName", serie_index))
}

// Return the last timeseries value from a TimeSeries Query
func Get_timeseries_query_value(json_timeseries gjson.Result, serie_index int) (float64, error) {
  if value, err := strconv.ParseFloat(Get_json_field(json_timeseries, fmt.Sprintf("items.0.timeSeries.%d.data.0.value", serie_index)), 64); err == nil {
    return value, nil
  } else {
    return -999999.999999, errors.New("Cannot parse timeseries value")
  }
}

// Return the number of different TimeSeries from a TimeSeriesQuery
func Get_timeseries_num(json_timeseries gjson.Result) (int, error) {
  if value, err := strconv.Atoi(Get_json_field(json_timeseries, "items.0.timeSeries.#")); err == nil {
    return value, nil
  } else {
    return -999999, errors.New("Cannot parse timeseries value")
  }
}
