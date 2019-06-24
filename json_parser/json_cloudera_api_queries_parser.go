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

  // Go JSON parsing libraries
	"github.com/tidwall/gjson"
)

// Base string to the Cloudera URL Query API
const API_BASE_URL="http://%s:%s/api/%s/%s"

// Compose the URL connection to the Cloudera API Query
func Build_api_query_url(host string, port string, version string, query string) string {
  return fmt.Sprintf(API_BASE_URL, host, port, version, query)
}

// Return the Num of items for a API Query
func Get_api_query_items_num(json_api gjson.Result) int {
  if value, err := strconv.Atoi(Get_json_field(json_api, "items.#")); err == nil {
    return value
  } else {
    return -1
  }
}

// Return the Host ID parameter for a API Query
func Get_api_query_host_id(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.hostId", serie_index))
}

// Return the Host ID parameter for a API Query
func Get_api_query_host_id_by_hostRef(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.hostRef.hostId", serie_index))
}

// Return the Host Name parameter for a API Query
func Get_api_query_host_name(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.hostname", serie_index))
}

// Return the Host IP Address parameter for a API Query
func Get_api_query_host_ip(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.ipAddress", serie_index))
}

// Return the Host Commission State parameter for a API Query
func Get_api_query_host_commission_state(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.commissionState", serie_index))
}

// Return the Host Maintenance Mode parameter for a API Query
func Get_api_query_host_maintenance_mode(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.maintenanceMode", serie_index))
}

// Return the Host Health Summary parameter for a API Query
func Get_api_query_host_health_summary(json_api gjson.Result) string {
  return Get_json_field (json_api, fmt.Sprintf("healthSummary"))
}

// Return the Cluster Name parameter for a API Query
func Get_api_query_cluster_name(json_api gjson.Result) string {
  return Get_json_field (json_api, "name")
}

// Return the Cluster Full Version parameter for a API Query
func Get_api_query_cluster_full_version(json_api gjson.Result) string {
  return Get_json_field (json_api, "fullVersion")
}

// Return the Cluster Entity Status parameter for a API Query
func Get_api_query_cluster_state(json_api gjson.Result) string {
  return Get_json_field (json_api, "entityStatus")
}

// Return the Cluster Maintenance Mode parameter for a API Query
func Get_api_query_cluster_maintenance_mode(json_api gjson.Result) string {
  return Get_json_field (json_api, "maintenanceMode")
}

// Return the Service Name parameter for a API Query
func Get_api_query_service_name(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.name", serie_index))
}

// Return the Service Type parameter for a API Query
func Get_api_query_service_type(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.type", serie_index))
}

// Return the Service State parameter for a API Query
func Get_api_query_service_state(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.serviceState", serie_index))
}

// Return the Service Health parameter for a API Query
func Get_api_query_service_health(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.healthSummary", serie_index))
}

// Return the Role Name parameter for a API Query
func Get_api_query_role_name(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.name", serie_index))
}

// Return the Role Type parameter for a API Query
func Get_api_query_role_type(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.type", serie_index))
}

// Return the Role State parameter for a API Query
func Get_api_query_role_state(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.roleState", serie_index))
}

// Return the Role Health parameter for a API Query
func Get_api_query_role_health(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("items.%d.healthSummary", serie_index))
}

// Return the Cloudera Management Service Name parameter for a API Query
func Get_api_query_cm_service_name(json_api gjson.Result) string {
  return Get_json_field (json_api, "name")
}

// Return the Cloudera Management Service Type parameter for a API Query
func Get_api_query_cm_service_type(json_api gjson.Result) string {
  return Get_json_field (json_api, "type")
}

// Return the Cloudera Management Service State parameter for a API Query
func Get_api_query_cm_service_state(json_api gjson.Result) string {
  return Get_json_field (json_api, "serviceState")
}

// Return the Cloudera Management Service Health parameter for a API Query
func Get_api_query_cm_service_health(json_api gjson.Result) string {
  return Get_json_field (json_api, "healthSummary")
}

// Return the Num of items for a API Query
func Get_api_query_cm_health_checks_num(json_api gjson.Result) int {
  if value, err := strconv.Atoi(Get_json_field(json_api, "healthChecks.#")); err == nil {
    return value
  } else {
    return -1
  }
}

// Return the Cloudera Management Service Name parameter for a API Query
func Get_api_query_cm_health_check_service_name(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("healthChecks.%d.name", serie_index))
}

// Return the Cloudera Management Service Name parameter for a API Query
func Get_api_query_cm_health_check_service_state(json_api gjson.Result, serie_index int) string {
  return Get_json_field (json_api, fmt.Sprintf("healthChecks.%d.summary", serie_index))
}

// Return A list of Clusters for a API Query
func Get_api_query_clusters_list(json_api gjson.Result) []gjson.Result {
  return Get_json_array (json_api, "items.#.displayName")
}

// Return the Cloudera Manager Version field
func Get_api_query_cm_version(json_api gjson.Result) string {
  return Get_json_field (json_api, "version")
}
