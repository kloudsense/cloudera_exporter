/*
 *
 * title           :collector/status_module.go
 * description     :Submodule Collector for the Cluster status metrcis
 * author		       :Raul Barroso (rbarroso@keedio.com)
 * co-author       :Alejandro Villegas Lopez (avillegas@keedio.com)
 * date            :2018/10/05
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
	"fmt"

  // Own libraries
  jp "keedio/cloudera_exporter/json_parser"
  log "keedio/cloudera_exporter/logger"

  // Go Prometheus libraries
	"github.com/prometheus/client_golang/prometheus"
)



/* ======================================================================
 * Data Structs
 * ====================================================================== */
// None



/* ======================================================================
 * Constants with the Host module TSquery sentences
 * ====================================================================== */
const STATUS_SCRAPER_NAME = "status"




/* ======================================================================
 * Global variables
 * ====================================================================== */
var  (
  // Cluster Status Metric Definition
	globalClusterDesc = prometheus.NewDesc(
      prometheus.BuildFQName(namespace, "status", "cluster_up"),
      "Cluster Up",
      []string{"cluster_name", "full_version", "cluster_state", "maintenance_mode"},
      nil,
  )

  // Host Status Metric Definition
	globalHostsDesc = prometheus.NewDesc(
      prometheus.BuildFQName(namespace, "status", "host_up"),
      "Host Up",
      []string{"host_id", "hostname", "ip", "commission_state", "maintenance_mode", "health_summary"},
      nil,
  )

  // Service Status Metric Definition
	globalServiceDesc = prometheus.NewDesc(
      prometheus.BuildFQName(namespace, "status", "service_up"),
      "Service Name up",
      []string{"service_name", "service_type", "service_state", "health_summary"},
      nil,
  )

  // Role Status Metric Definition
	globalRoleDesc = prometheus.NewDesc(
      prometheus.BuildFQName(namespace, "status", "role_up"),
      "Role Name up",
      []string{"role_name", "host_id", "host_name", "role_type", "role_state", "health_summary", "service"},
      nil,
  )
  scrapeError float64
)





/* ======================================================================
 * Functions
 * ====================================================================== */
// Function to get a value for a State
func get_value_from_state (state string) float64 {
  retval := -1.0
  switch state{
  case "NOT_AVAILABLE":
    retval = 0.0
  case "HISTORY_NOT_AVAILABLE":
    retval = 0.0
  case "NONE":
    retval = 0.0
  case "BAD":
    retval = 1.0
  case "BAD_HEALTH":
    retval = 1.0
  case "STOPPED":
    retval = 1.0
  case "DOWN":
    retval = 1.0
  case "UNKNOWN":
    retval = 2.0
  case "UNKNOWN_HEALTH":
    retval = 2.0
  case "STOPPING":
    retval = 2.0
  case "STARTING":
    retval = 2.0
  case "DISABLED":
    retval = 3.0
  case "DISABLED_HEALTH":
    retval = 3.0
  case "CONCERNING":
    retval = 4.0
  case "CONCERNING_HEALTH":
    retval = 4.0
  case "GOOD":
    retval = 5.0
  case "GOOD_HEALTH":
    retval = 5.0
  default:
    retval = -1.0
  }
  return retval
}

// Function to Scrape the Hosts Status Metrics
func scrape_cluster_hosts_status(ctx context.Context, config Collector_connection_data, query string, ch chan<- prometheus.Metric) bool {
  json_parsed, err := make_and_parse_api_query(ctx, config, query)
  if err != nil {
    return false
  }

  // For each Host
  number_services := jp.Get_api_query_items_num(json_parsed)
  for counter_hosts := 0; counter_hosts < int(number_services); counter_hosts++ {
    host_id := jp.Get_api_query_host_id(json_parsed, counter_hosts)
    host_name := jp.Get_api_query_host_name(json_parsed, counter_hosts)
    host_ip := jp.Get_api_query_host_ip(json_parsed, counter_hosts)
    host_commission_state := jp.Get_api_query_host_commission_state(json_parsed, counter_hosts)
    host_maintenance_mode := jp.Get_api_query_host_maintenance_mode(json_parsed, counter_hosts)
    query_by_host := fmt.Sprintf("%s/%s", query, host_id)
    json_parsed_by_host, _ := make_and_parse_api_query(ctx, config, query_by_host)
    host_health_summary := jp.Get_api_query_host_health_summary(json_parsed_by_host)
    host_healt_summary_value := get_value_from_state(host_health_summary)
    ch <- prometheus.MustNewConstMetric(globalHostsDesc, prometheus.GaugeValue, host_healt_summary_value, host_id, host_name, host_ip, host_commission_state, host_maintenance_mode, host_health_summary)
  }
  return true
}

// Function to Scrape the Cluster Status Metric
func scrape_cluster_status(ctx context.Context, config Collector_connection_data, query string, ch chan<- prometheus.Metric) bool {
  json_parsed, err := make_and_parse_api_query(ctx, config, query)
  if err != nil {
    return false
  }

  // Cluster Metrics
  cluster_name := jp.Get_api_query_cluster_name(json_parsed)
  cluster_full_version := jp.Get_api_query_cluster_full_version(json_parsed)
  cluster_state := jp.Get_api_query_cluster_state(json_parsed)
  cluster_maintenance_mode := jp.Get_api_query_cluster_maintenance_mode(json_parsed)
  cluster_state_value := get_value_from_state(cluster_state)
  ch <- prometheus.MustNewConstMetric(globalClusterDesc, prometheus.GaugeValue, cluster_state_value, cluster_name, cluster_full_version, cluster_state, cluster_maintenance_mode)
  return true
}

// Function to Scrape the Services Status Metrics
func scrape_cluster_services_status(ctx context.Context, config Collector_connection_data, query string, ch chan<- prometheus.Metric) bool {
  json_parsed, err := make_and_parse_api_query(ctx, config, query)
  if err != nil {
    return false
  }

  num_services := jp.Get_api_query_items_num(json_parsed)
  for counter_services := 0; counter_services < int(num_services); counter_services++ {
    service_name := jp.Get_api_query_service_name(json_parsed, counter_services)
    service_type := jp.Get_api_query_service_type(json_parsed, counter_services)
    service_state := jp.Get_api_query_service_state(json_parsed, counter_services)
    health_summary := jp.Get_api_query_service_health(json_parsed, counter_services)
    service_state_value := get_value_from_state(health_summary)
    ch <- prometheus.MustNewConstMetric(globalServiceDesc, prometheus.GaugeValue, service_state_value, service_name, service_type, service_state, health_summary)
  }
  return true
}

// Function to Scrape the Cloudera Management Services Status Metrics
func scrape_cluster_cm_services_status(ctx context.Context, config Collector_connection_data, query string, ch chan<- prometheus.Metric) bool {
  // Cloudera Management Services
  json_parsed, err := make_and_parse_api_query(ctx, config, query)
  if err != nil {
    return false
  }

  // Cloudera Management 
  cm_service_name := jp.Get_api_query_cm_service_name(json_parsed)
  cm_service_type := jp.Get_api_query_cm_service_type(json_parsed)
  cm_service_state := jp.Get_api_query_cm_service_state(json_parsed)
  cm_health_summary := jp.Get_api_query_cm_service_health(json_parsed)
  cm_service_value := get_value_from_state(cm_health_summary)
  ch <- prometheus.MustNewConstMetric(globalServiceDesc, prometheus.GaugeValue, cm_service_value, cm_service_name, cm_service_type, cm_service_state, cm_health_summary)

  // Cloudera Management SubServices 
  num_services := jp.Get_api_query_cm_health_checks_num(json_parsed)
  for counter_services := 0; counter_services < num_services; counter_services++ {
    service_name := jp.Get_api_query_cm_health_check_service_name(json_parsed, counter_services)
    service_state := jp.Get_api_query_cm_health_check_service_state(json_parsed, counter_services)
    service_value := get_value_from_state(service_state)
    ch <- prometheus.MustNewConstMetric(globalServiceDesc, prometheus.GaugeValue, service_value, service_name, cm_service_type, service_state, service_state)
  }
  return true
}

// Function that returns to a map with the hostName and HostId
func scrape_hostName(ctx context.Context, config Collector_connection_data, query string) map[string]string {
  json_parsed,err:= make_and_parse_api_query(ctx, config, query)
  if err != nil {

  }
  // For each Host
  number_services := jp.Get_api_query_items_num(json_parsed)
  mapHost := make(map[string]string)
  for counter_hosts := 0; counter_hosts < int(number_services); counter_hosts++ {
    host_id := jp.Get_api_query_host_id(json_parsed, counter_hosts)
    host_name := jp.Get_api_query_host_name(json_parsed, counter_hosts)
    mapHost[host_id] = host_name
  }
  return  mapHost
}
// Function that returns the name of the host given the HostId
func Get_hostName_with_hostId( mapHosName map[string]string, hostId string) string {

  string_hostid := mapHosName[hostId]

  return  string_hostid
}


// Function to Scrape the Roles Status Metrics
func scrape_cluster_roles_status(ctx context.Context, config Collector_connection_data, query string, ch chan<- prometheus.Metric) bool{
  json_parsed_service, err := make_and_parse_api_query(ctx, config, query)
  mapHost := scrape_hostName(ctx, config, "hosts")

  if err != nil {
    return false
  }

  num_services := jp.Get_api_query_items_num(json_parsed_service)
  for counter_services := 0; counter_services < int(num_services); counter_services++ {
    service_name := jp.Get_api_query_service_name(json_parsed_service, counter_services)
    json_parsed_roles, _ := make_and_parse_api_query(ctx, config, fmt.Sprintf("%s/%s/roles", query, service_name))
    num_roles := jp.Get_api_query_items_num(json_parsed_roles)
    for counter_roles := 0; counter_roles < int(num_roles); counter_roles++ {
      role_name := jp.Get_api_query_role_name(json_parsed_roles, counter_roles)
      host_id := jp.Get_api_query_host_id_by_hostRef(json_parsed_roles, counter_roles)
      host_name := Get_hostName_with_hostId(mapHost, host_id)
      role_type := jp.Get_api_query_role_type(json_parsed_roles, counter_roles)
      role_state := jp.Get_api_query_role_state(json_parsed_roles, counter_roles)
      health_summary := jp.Get_api_query_role_health(json_parsed_roles, counter_roles)
      role_state_value := get_value_from_state(health_summary)
      ch <- prometheus.MustNewConstMetric(globalRoleDesc, prometheus.GaugeValue, role_state_value, role_name, host_id, host_name, role_type, role_state, health_summary, service_name)
    }
  }
  return true
}

// Eval the return code of the scrape functions to determine if are correct or with errors
func eval_scrape(retval bool, success_queries *int, error_queries *int) {
  if retval {
    *success_queries += 1
  } else {
    *error_queries +=1
  }
}




/* ======================================================================
 * Scrape "Class"
 * ====================================================================== */
// ScrapeStatus collects from /clusters/hosts.
type ScrapeStatus struct {}

// Name of the Scraper. Should be unique.
func (sgs ScrapeStatus) Name() string {
  return "status_collector"
}

// Help describes the role of the Scraper.
func (sgs ScrapeStatus) Help() string {
  return "Collector para la recoleccion de las metricas de estado del Cluster, servicios, Cloudera Management, hosts y roles"
}

// Scraper Version.
func (sgs ScrapeStatus) Version() float64 {
  return 1.0
}


// Scraper Main Function
func (ScrapeStatus) Scrape (ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error {
  log.Debug_msg("Ejecutando Status Metrics Scraper")

  // Queries counters
  success_queries := 0
  error_queries := 0

  // Get Clusters list
  json_clusters, err := make_and_parse_api_query(ctx, *config, "clusters")
  if err != nil {
    return nil
  }

  eval_scrape(scrape_cluster_hosts_status(ctx, *config, "hosts", ch), &success_queries, &error_queries)
  eval_scrape(scrape_cluster_cm_services_status(ctx, *config, "cm/service", ch), &success_queries, &error_queries)

  clustersName := jp.Get_api_query_clusters_list(json_clusters)
  for c_clusters := 0; c_clusters < len(clustersName); c_clusters++ {
    cluster := clustersName[c_clusters].String()

    eval_scrape(scrape_cluster_status(ctx, *config, fmt.Sprintf("clusters/%s", cluster), ch), &success_queries, &error_queries)
    eval_scrape(scrape_cluster_services_status(ctx, *config, fmt.Sprintf("clusters/%s/services", cluster), ch), &success_queries, &error_queries)
    eval_scrape(scrape_cluster_roles_status(ctx, *config, fmt.Sprintf("clusters/%s/services", cluster), ch), &success_queries, &error_queries)
  }
  log.Debug_msg("In the Status Module has been executed %d queries. %d success and %d with errors", success_queries + error_queries, success_queries, error_queries)
  return nil
}

var _ Scraper = ScrapeStatus{}
