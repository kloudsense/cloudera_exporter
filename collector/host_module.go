/*
 *
 * title           :collector/host.go
 * description     :Submodule Collector for the Cluster hosts metrics
 * author		       :Alejandro Villegas
 * date            :2019/02/04
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
	"strings"

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
const HOST_SCRAPER_NAME = "host"
const (
  // Agent Queries
  HOST_AGENT_CPU_SYSTEM_PERCENT_QUERY= "SELECT LAST(INTEGRAL(agent_cpu_system_rate)) WHERE category=HOST"
  HOST_AGENT_CPU_USER_PERCENT_QUERY=   "SELECT LAST(INTEGRAL(agent_cpu_user_rate)) WHERE category=HOST"
  HOST_AGENT_VIRT_MEM_USE_QUERY=       "SELECT LAST(agent_virtual_memory_used) WHERE category=HOST"
  HOST_AGENT_PHYS_MEM_USE_QUERY=       "SELECT LAST(agent_physical_memory_used) WHERE category=HOST"

  // CPU Queries
  HOST_CPU_CORES_QUERY=                "SELECT LAST(cores) WHERE CATEGORY=HOST"
  HOST_CPU_IDLE_PERCENT_QUERY=         "SELECT (LAST(INTEGRAL(cpu_idle_rate))*100/60) / LAST(cores) WHERE CATEGORY=HOST"
  HOST_CPU_IOWAIT_PERCENT_QUERY=       "SELECT (LAST(INTEGRAL(cpu_iowait_rate))*100/60) / LAST(cores) WHERE CATEGORY=HOST"
  HOST_CPU_LOAD15_QUERY=               "SELECT LAST(load_1) WHERE CATEGORY=HOST"
  HOST_CPU_LOAD1_QUERY=                "SELECT LAST(load_15) WHERE CATEGORY=HOST"
  HOST_CPU_LOAD5_QUERY=                "SELECT LAST(load_5) WHERE CATEGORY=HOST"
  HOST_CPU_PERCENT_QUERY=              "SELECT LAST(cpu_percent) WHERE CATEGORY=HOST"
  HOST_CPU_SYSTEM_PERCENT_QUERY=       "SELECT (LAST(INTEGRAL(cpu_system_rate))*100/60) / LAST(cores) WHERE CATEGORY=HOST"
  HOST_CPU_USER_PERCENT_QUERY=         "SELECT (LAST(INTEGRAL(cpu_user_rate))*100/60) / LAST(cores) WHERE CATEGORY=HOST"

  // RAM MEM Queries
  HOST_MEM_FREE_QUERY=                 "SELECT LAST(physical_memory_memfree) WHERE CATEGORY=HOST"
  HOST_MEM_TOTAL_QUERY=                "SELECT LAST(physical_memory_total) WHERE CATEGORY=HOST"
  HOST_MEM_USED_QUERY=                 "SELECT LAST(physical_memory_used) WHERE CATEGORY=HOST"
  HOST_MEM_WRITE_BACK_QUERY=           "SELECT LAST(physical_memory_writeback) WHERE CATEGORY=HOST"

  // SWAP MEM Queries
  HOST_SWAP_FREE_QUERY=                "SELECT LAST(swap_free) WHERE CATEGORY=HOST"
  HOST_SWAP_OUT_QUERY=                 "SELECT LAST(INTEGRAL(swap_out_rate)) WHERE CATEGORY=HOST"
  HOST_SWAP_TOTAL_QUERY=               "SELECT LAST(swap_total) WHERE CATEGORY=HOST"
  HOST_SWAP_USED_QUERY=                "SELECT LAST(swap_used) WHERE CATEGORY=HOST"

  // Other Queries
  HOST_OTHER_CLOCK_OFFSET_QUERY=       "SELECT LAST(clock_offset) WHERE category=HOST"
  HOST_OTHER_ALERTS_QUERY=             "SELECT LAST(INTEGRAL(alerts_rate)) WHERE category=HOST"
  HOST_OTHER_DNS_RESOLUTION_TIME=      "SELECT LAST(dns_name_resolution_duration) WHERE category=HOST"
  HOST_OTHER_UPTIME=                   "SELECT LAST(uptime) WHERE category=HOST"
)




/* ======================================================================
 * Global variables
 * ====================================================================== */
// Prometheus data Descriptors for the metrics to export
var type_node_list map[string] []string
var (
  // Agent Metrics
  global_host_agent_cpu_system_percent = create_host_metric_struct("agent_cpu_system_percent", "Agent CPU System Percent")
  global_host_agent_cpu_user_percent = create_host_metric_struct("agent_cpu_user_percent", "Agent CPU User Percent")
  global_host_agent_phys_mem_use = create_host_metric_struct("agent_phys_mem_use", "Agent Physical Memory Use")
  global_host_agent_virt_mem_use = create_host_metric_struct("agent_virt_mem_use", "Agent Virtual Memory Usage")

  // CPU Metrics
  global_host_cpu_cores = create_host_metric_struct("cpu_cores",  "CPU Cores")
	global_host_cpu_iddle_percent = create_host_metric_struct( "cpu_iddle_percent",  "CPU Iddle time percent")
  global_host_cpu_iowait_percent = create_host_metric_struct("cpu_iowait_percent", "CPU IOWAIT time percent")
  global_host_cpu_load15 = create_host_metric_struct("load_15_by_host", "Load 15 Min By Host")
  global_host_cpu_load1 = create_host_metric_struct("load_1_by_host", "Load 1 Min By Host")
  global_host_cpu_load5 = create_host_metric_struct("load_5_by_host", "Load 5 Min By Host")
  global_host_cpu_percent = create_host_metric_struct("cpu_percent_by_host", "CPU Percent By Host")
  global_host_cpu_system_percent = create_host_metric_struct("cpu_system_percent", "CPU System time percent")
  global_host_cpu_user_percent = create_host_metric_struct("cpu_user_percent", "CPU User time percent")

  // RAM MEM Metrics
  global_host_mem_free = create_host_metric_struct("mem_free_by_host", "Free Mem By Host")
  global_host_mem_total = create_host_metric_struct("mem_total_by_host", "Total Mem By Host")
  global_host_mem_used = create_host_metric_struct("mem_used_by_host", "Used Mem By Host")
  global_host_mem_write_back = create_host_metric_struct("mem_writeback_by_host", "Write Back Mem By Host")

  // SWAP MEM Metrics
  global_host_swap_free = create_host_metric_struct("swap_free_by_host", "Free Swap By Host")
  global_host_swap_out = create_host_metric_struct("swap_out_by_host", "Out Swap By Host: Memory swapped to Disk")
  global_host_swap_total = create_host_metric_struct("swap_total_by_host", "Total Swap By Host")
  global_host_swap_used = create_host_metric_struct("swap_used_by_host", "Used Swap By Host")

  // Other Queries
  global_host_other_clock_offset = create_host_metric_struct("clock_offset", "Clock-Offset By Host")
  global_host_other_alerts = create_host_metric_struct("alerts", "Alerts By Host")
  global_host_other_dns_resolution_time = create_host_metric_struct("dns_resolution_time", "DNS Resolution time By Host")
  global_host_other_uptime = create_host_metric_struct("uptime", "Uptime By Host")
)

// Creation of the structure that relates the queries with the descriptors of the Prometheus metrics
var host_query_variable_relationship = []relation {
  {HOST_AGENT_CPU_SYSTEM_PERCENT_QUERY, *global_host_agent_cpu_system_percent},
  {HOST_AGENT_CPU_USER_PERCENT_QUERY,   *global_host_agent_cpu_user_percent},
  {HOST_AGENT_PHYS_MEM_USE_QUERY,       *global_host_agent_phys_mem_use},
  {HOST_AGENT_VIRT_MEM_USE_QUERY,       *global_host_agent_virt_mem_use},
  {HOST_CPU_CORES_QUERY,                *global_host_cpu_cores},
  {HOST_CPU_IDLE_PERCENT_QUERY,         *global_host_cpu_iddle_percent},
  {HOST_CPU_IOWAIT_PERCENT_QUERY,       *global_host_cpu_iowait_percent},
  {HOST_CPU_LOAD15_QUERY,               *global_host_cpu_load15},
  {HOST_CPU_LOAD1_QUERY,                *global_host_cpu_load1},
  {HOST_CPU_LOAD5_QUERY,                *global_host_cpu_load5},
  {HOST_CPU_PERCENT_QUERY,              *global_host_cpu_percent},
  {HOST_CPU_SYSTEM_PERCENT_QUERY,       *global_host_cpu_system_percent},
  {HOST_CPU_USER_PERCENT_QUERY,         *global_host_cpu_user_percent},
  {HOST_MEM_FREE_QUERY,                 *global_host_mem_free},
  {HOST_MEM_TOTAL_QUERY,                *global_host_mem_total},
  {HOST_MEM_USED_QUERY,                 *global_host_mem_used},
  {HOST_MEM_WRITE_BACK_QUERY,           *global_host_mem_write_back},
  {HOST_OTHER_ALERTS_QUERY,             *global_host_other_alerts},
  {HOST_OTHER_CLOCK_OFFSET_QUERY,       *global_host_other_clock_offset},
  {HOST_OTHER_DNS_RESOLUTION_TIME,      *global_host_other_dns_resolution_time},
  {HOST_OTHER_UPTIME,                   *global_host_other_uptime},
  {HOST_SWAP_FREE_QUERY,                *global_host_swap_free},
  {HOST_SWAP_OUT_QUERY,                 *global_host_swap_out},
  {HOST_SWAP_TOTAL_QUERY,               *global_host_swap_total},
  {HOST_SWAP_USED_QUERY,                *global_host_swap_used},
}




/* ======================================================================
 * Functions
 * ====================================================================== */
// Create and returns a prometheus descriptor for a host metric. 
// The "metric_name" parameter its mandatory
// If the "description" parameter is empty, the function assings it with the
// value of the name of the metric in uppercase and separated by spaces
func create_host_metric_struct(metric_name string, description string) *prometheus.Desc {
  // Correct "description" parameter if is empty
  if len(description) == 0 {
    description = strings.Replace(strings.ToUpper(metric_name), "_", " ", 0)
  }

  // return prometheus descriptor
  return prometheus.NewDesc(
    prometheus.BuildFQName(namespace, HOST_SCRAPER_NAME, metric_name),
    description,
    []string{"cluster", "hostname", "hostid", "is_master_node", "is_border_node", "is_worker_node"},
    nil,
  )
}


// Generic function to extract de metadata associated with the query value
// Only for Host metric type
// For this module, the cluster to which the host belongs is indifferent.  The
// name of the cluster to which the host belongs is associated as metadata to
// its corresponding metric
func create_host_metric (ctx context.Context, config Collector_connection_data, query string, metric_struct prometheus.Desc, ch chan<- prometheus.Metric) bool {
  // Make the query
  json_parsed, err := make_and_parse_timeseries_query(ctx, config, query)
  if err != nil {
    return false
  }

  // Get the num of hosts in the cluster or clusters
  num_hosts, err := jp.Get_timeseries_num(json_parsed)
  if err != nil {
    return false
  }

  // Extract Metadata for each host
  for host_index := 0; host_index < num_hosts; host_index ++ {
    // Get Host ID
    host_id := jp.Get_timeseries_query_host_id(json_parsed, host_index)
    // Get Host Name
    host_name := jp.Get_timeseries_query_host_name(json_parsed, host_index)
    // Get Cluster Name
    cluster_name := jp.Get_timeseries_query_cluster(json_parsed, host_index)
    // Get the flag to determine if the host is a Master Node
    is_master_node := get_if_is_master(host_id)
    // Get the flag to determine if the host is a Border Node
    is_border_node := get_if_is_border(host_id)
    // Get the flag to determine if the host is a Worker Node
    is_worker_node := get_if_is_worker(host_id)
    // Get Query LAST value
    value, err := jp.Get_timeseries_query_value(json_parsed, host_index)
    if err != nil {
	continue
    }
    // Assing the data to the Prometheus descriptor
    ch <- prometheus.MustNewConstMetric(&metric_struct, prometheus.GaugeValue, value, cluster_name, host_name, host_id, is_master_node, is_border_node, is_worker_node)
  }
  return true
}




/* ======================================================================
 * Scrape "Class"
 * ====================================================================== */
// ScrapeHost struct
type ScrapeHost struct{}

// Name of the Scraper. Should be unique.
func (ScrapeHost) Name() string {
  return HOST_SCRAPER_NAME
}

// Help describes the role of the Scraper.
func (ScrapeHost) Help() string {
  return "Host Metrics"
}

// Version.
func (ScrapeHost) Version() float64 {
  return 1.0
}

// Scrape generic function. Override for host module.
func (ScrapeHost) Scrape (ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error {
  log.Debug_msg("Ejecutando Hosts Metrics Scraper")

  // Make the list of the Hosts Types (Master, Worker, Border)
  type_node_list = get_type_node_list(ctx, *config)

  // Queries counters
  success_queries := 0
  error_queries := 0

  // Execute the generic funtion for creation of metrics with the pairs (QUERY, PROM:DESCRIPTOR)
  for i:=0 ; i < len(host_query_variable_relationship) ; i++ {
    if create_host_metric(ctx, *config, host_query_variable_relationship[i].Query, host_query_variable_relationship[i].Metric_struct, ch) {
      success_queries += 1
    } else {
      error_queries += 1
    }
  }
  log.Debug_msg("In the Host Module has been executed %d queries. %d success and %d with errors", success_queries + error_queries, success_queries, error_queries)
  return nil
}

var _ Scraper = ScrapeHost{}
