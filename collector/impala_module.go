/*
 *
 * title           :collector/hdfs_module.go
 * description     :Submodule Collector for the Cluster HDFS metrics
 * author               :Raul Barroso
 * co-author           :Alejandro Villegas
 * date            :2019/02/11
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

type relationa struct {
  Query *string
  Metric_struct prometheus.Desc
}



/* ======================================================================
 * Constants with the Host module TSquery sentences
 * ====================================================================== */
const IMPALA_SCRAPER_NAME = "impala"
var (
  // Agent Queries
  IMPALA_CATALOG_JVM_COMITTED_BYTES =           "SELECT LAST(impala_catalogserver_jvm_heap_committed_usage_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_CATALOG_JVM_CURRENT_BYTES =            "SELECT LAST(impala_catalogserver_jvm_heap_current_usage_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_CATALOG_JVM_INIT_BYTES =               "SELECT LAST(impala_catalogserver_jvm_heap_init_usage_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_CATALOG_JVM_MAX_BYTES =                "SELECT LAST(impala_catalogserver_jvm_heap_max_usage_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_MEM_PAGE_CACHE =                "SELECT LAST(cgroup_mem_page_cache) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_MEM_RSS =                       "SELECT LAST(cgroup_mem_rss) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_MEM_SWAP =                      "SELECT LAST(cgroup_mem_swap) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_READ_IOSRATE =                  "SELECT LAST(INTEGRAL(cgroup_read_ios_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_READ_RATE =                     "SELECT LAST(INTEGRAL(cgroup_read_bytes_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_SYSTEM_RATE =                   "SELECT LAST(INTEGRAL(cgroup_cpu_system_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_USER_RATE =                     "SELECT LAST(INTEGRAL(cgroup_cpu_user_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_WRITE_IOSRATE =                 "SELECT LAST(INTEGRAL(cgroup_write_ios_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_CGROUP_WRITE_RATE =                    "SELECT LAST(INTEGRAL(cgroup_write_bytes_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_MEM_RSS =                              "SELECT LAST(mem_rss) WHERE serviceType = \"IMPALA\""
  IMPALA_MEM_SWAP =                             "SELECT LAST(mem_swap) WHERE serviceType = \"IMPALA\""
  IMPALA_MEM_VIRT =                             "SELECT LAST(mem_virtual) WHERE serviceType = \"IMPALA\""
  IMPALA_OOMEXIT =                              "SELECT LAST(INTEGRAL(oom_exits_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_ADMISSION_WAIT_RATE =            "SELECT LAST(INTEGRAL(impala_query_admission_wait_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_BYTES_HDFS_READ_RATE =           "SELECT LAST(INTEGRAL(impala_query_hdfs_bytes_read_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_BYTES_HDFS_WRITTE_RATE =         "SELECT LAST(INTEGRAL(impala_query_hdfs_bytes_written_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_BYTES_STREAMED_RATE =            "SELECT LAST(INTEGRAL(impala_query_bytes_streamed_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_CM_CPU =                         "SELECT LAST(INTEGRAL(impala_query_cm_cpu_milliseconds_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_DURATION_RATE =                  "SELECT LAST(INTEGRAL(impala_query_query_duration_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_INGESTED_RATE =                  "SELECT LAST(INTEGRAL(queries_ingested_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_MEM_ACCRUAL_RATE =               "SELECT LAST(INTEGRAL(impala_query_memory_accrual_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_MEM_SPILLED_RATE =               "SELECT LAST(INTEGRAL(impala_query_memory_spilled_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_OOMRATE =                        "SELECT LAST(INTEGRAL(queries_oom_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_REJECTED_RATE =                  "SELECT LAST(INTEGRAL(queries_rejected_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_SPILLED_RATE =                   "SELECT LAST(INTEGRAL(queries_spilled_memory_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_SUCCESSFUL_RATE =                "SELECT LAST(INTEGRAL(queries_successful_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_THREAD_CPU_RATE =                "SELECT LAST(INTEGRAL(impala_query_thread_cpu_time_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_QUERY_TIME_OUT_RATE =                  "SELECT LAST(INTEGRAL(queries_timed_out_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_READ_RATE =                            "SELECT LAST(INTEGRAL(read_bytes_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_CACHE_TOTAL_CLIENTS =      "SELECT LAST(statestore_subscriber_statestore_client_cache_total_clients) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_CLIENTS_IN_USE =           "SELECT LAST(statestore_subscriber_statestore_client_cache_clients_in_use) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_LAST =          "SELECT LAST(statestore_subscriber_heartbeat_interval_time_last) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_MAX =           "SELECT LAST(statestore_subscriber_heartbeat_interval_time_max) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_MEAN =          "SELECT LAST(statestore_subscriber_heartbeat_interval_time_mean) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_MIN =           "SELECT LAST(statestore_subscriber_heartbeat_interval_time_min) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_RATE =          "SELECT LAST(INTEGRAL(statestore_subscriber_heartbeat_interval_time_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_HEART_BEAT_STDDEV =        "SELECT LAST(statestore_subscriber_heartbeat_interval_time_stddev) WHERE serviceType = \"IMPALA\""
  IMPALA_STATE_STORE_LAST_RECOVERY_DURATION =   "SELECT LAST(statestore_subscriber_last_recovery_duration) WHERE serviceType = \"IMPALA\""
  IMPALA_TCMALLOC_FREE_BYTES =                  "SELECT LAST(tcmalloc_pageheap_free_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_TCMALLOC_PHYSICAL_RESERVED_BYTES =     "SELECT LAST(tcmalloc_physical_bytes_reserved) WHERE serviceType = \"IMPALA\""
  IMPALA_TCMALLOC_TOTAL_RESERVED_BYTES =        "SELECT LAST(tcmalloc_total_bytes_reserved) WHERE serviceType = \"IMPALA\""
  IMPALA_TCMALLOC_UNMAPPED_BYTES =              "SELECT LAST(tcmalloc_pageheap_unmapped_bytes) WHERE serviceType = \"IMPALA\""
  IMPALA_TCMALLOC_USED_BYTES =                  "SELECT LAST(tcmalloc_bytes_in_use) WHERE serviceType = \"IMPALA\""
  IMPALA_THRIFT_CONNECTIONS_RATE =              "SELECT LAST(INTEGRAL(thrift_server_catalog_service_connections_rate)) WHERE serviceType = \"IMPALA\""
  IMPALA_THRIFT_CONNECTIONS_USED =              "SELECT LAST(thrift_server_catalog_service_connections_in_use) WHERE serviceType = \"IMPALA\""
  IMPALA_WRITE_RATE =                           "SELECT LAST(INTEGRAL(write_bytes_rate)) WHERE serviceType = \"IMPALA\""
)




/* ======================================================================
 * Global variables
 * ====================================================================== */
var (
  impala_catalog_jvm_comitted_bytes =          create_impala_metric_struct("impala_catalogserver_jvm_heap_committed_usage_bytes", "Jvm heap Committed Usage in Bytes.")
  impala_catalog_jvm_current_bytes =           create_impala_metric_struct("impala_catalogserver_jvm_heap_current_usage_bytes", "Jvm heap Current Usage in Bytes.")
  impala_catalog_jvm_init_bytes =              create_impala_metric_struct("impala_catalogserver_jvm_heap_init_usage_bytes", "JVM heap Init Usage in Bytes.")
  impala_catalog_jvm_max_bytes =               create_impala_metric_struct("impala_catalogserver_jvm_heap_max_usage_bytes", "JVM heap Max Usage in Bytes.")
  impala_cgroup_mem_page_cache =               create_impala_metric_struct("cgroup_mem_page_cache", "Page cache usage of the role's cgroup in Bytes.")
  impala_cgroup_mem_rss =                      create_impala_metric_struct("cgroup_mem_rss", "Resident memory of the role's cgroup in Bytes.")
  impala_cgroup_mem_swap =                     create_impala_metric_struct("cgroup_mem_swap", "Swap usage of the role's cgroup in Bytes.")
  impala_cgroup_read_iosrate =                 create_impala_metric_struct("cgroup_read_ios_rate", "Number of read I/O operations from all disks by the role's cgroup.")
  impala_cgroup_read_rate =                    create_impala_metric_struct("cgroup_read_bytes_rate", "Bytes read from all disks by the role's cgroup in Bytes per second.")
  impala_cgroup_system_rate =                  create_impala_metric_struct("cgroup_cpu_system_rate", "CPU usage of the role's cgroup in Bytes per second.")
  impala_cgroup_user_rate =                    create_impala_metric_struct("cgroup_cpu_user_rate", "User Space CPU usage of the role's cgroup in Bytes per second.")
  impala_cgroup_write_iosrate =                create_impala_metric_struct("cgroup_write_ios_rate", "Number of write I/O operations to all disks by the role's cgroup.")
  impala_cgroup_write_rate =                   create_impala_metric_struct("cgroup_write_bytes_rate", "Bytes written to all disks by the role's cgroup.")
  impala_mem_rss =                             create_impala_metric_struct("mem_rss", "Resident memory used in Bytes")
  impala_mem_swap =                            create_impala_metric_struct("mem_swap", "Amount of swap memory used by this role's process in Bytes")
  impala_mem_virt =                            create_impala_metric_struct("mem_virtual", "Virtual memory used in Bytes.")
  impala_oomexit =                             create_impala_metric_struct("oom_exits_rate", "The number of times the role's backing process was killed due to an OutOfMemory error. This counter is only incremented if the Cloudera Manager \"Kill When Out of Memory\" option is enabled.")
  impala_query_admission_wait_rate =           create_impala_metric_struct("impala_query_admission_wait_rate", "The time from submission for admission to its completion in milliseconds.")
  impala_query_bytes_hdfs_read_rate =          create_impala_metric_struct("impala_query_hdfs_bytes_read_rate", "The total number of bytes read from HDFS by this Impala query.")
  impala_query_bytes_hdfs_writte_rate =        create_impala_metric_struct("impala_query_hdfs_bytes_written_rate", "The total number of bytes written to HDFS by this Impala query.")
  impala_query_bytes_streamed_rate =           create_impala_metric_struct("impala_query_bytes_streamed_rate", "The total number of bytes sent between Impala Daemons while processing this query.")
  impala_query_cm_cpu =                        create_impala_metric_struct("impala_query_cm_cpu_milliseconds_rate", "impala.analysis.cm_cpu_milliseconds.description.")
  impala_query_duration_rate =                 create_impala_metric_struct("impala_query_query_duration_rate", "The duration of the query in milliseconds.")
  impala_query_ingested_rate =                 create_impala_metric_struct("queries_ingested_rate", "Impala queries ingested by the Service Monitor")
  impala_query_mem_accrual_rate =              create_impala_metric_struct("impala_query_memory_accrual_rate", "The total accrued memory usage by the query. This is computed by multiplaying the average aggregate memory usage of the query by the query's duration.")
  impala_query_mem_spilled_rate =              create_impala_metric_struct("impala_query_memory_spilled_rate", "Amount of memory spilled to disk in Bytes.")
  impala_query_oomrate =                       create_impala_metric_struct("queries_oom_rate", "Number of Impala queries for which memory consumption exceeded what was allowed")
  impala_query_rejected_rate =                 create_impala_metric_struct("queries_rejected_rate", "Number of Impala queries rejected from admission, commonly due to the queue being full or insufficient memory")
  impala_query_spilled_rate =                  create_impala_metric_struct("queries_spilleed_rate", "Number of Impala queries that spilled to disk")
  impala_query_successful_rate =               create_impala_metric_struct("queries_successful_rate", "Number of Impala queries that ran to completion successfully")
  impala_query_thread_cpu_rate =               create_impala_metric_struct("impala_query_thread_cpu_time_rate", "The sum of the CPU time used by all threads of the query.")
  impala_query_time_out_rate =                 create_impala_metric_struct("queries_time_out_rate", "Impala queries that timed out waiting in queue during admission in milliseconds")
  impala_read_rate =                           create_impala_metric_struct("read_bytes_rate", "The number of bytes read from the device.")
  impala_state_store_cache_total_clients =     create_impala_metric_struct("statestore_subscriber_statestore_client_cache_total_clients", "The total number of StateStore subscriber clients in this Impala Daemon's client cache. These clients are for communication from this role to the StateStore.")
  impala_state_store_clients_in_use =          create_impala_metric_struct("statestore_subscriber_statestore_client_cache_clients_in_use", "The number of active StateStore subscriber clients in this Impala Daemon's client cache. These clients are for communication from this role to the StateStore.")
  impala_state_store_heart_beat_last =         create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_last", "The most recent interval between heartbeats from this Impala Daemon to the StateStore in seconds.")
  impala_state_store_heart_beat_max =          create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_max", " The maximum interval between heartbeats from this Impala Daemon to the StateStore in seconds. This is calculated over the lifetime of the Impala Daemon.")
  impala_state_store_heart_beat_mean =         create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_mean", " The average interval between heartbeats from this Impala Daemon to the StateStore in seconds. This is calculated over the lifetime of the Impala Daemon.")
  impala_state_store_heart_beat_min =          create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_min", "The minimum interval between heartbeats from this Impala Daemon to the StateStore in seconds. This is calculated over the lifetime of the Impala Daemon.")
  impala_state_store_heart_beat_rate =         create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_rate", "The total number of samples taken of the Impala Daemon's StateStore heartbeat interval in samples per second.")
  impala_state_store_heart_beat_stddev =       create_impala_metric_struct("statestore_subscriber_heartbeat_interval_time_stddev", "The standard deviation in the interval between heartbeats from this Impala Daemon to the StateStore in seconds. This is calculated over the lifetime of the Impala Daemon.")
  impala_state_store_last_recovery_duration =  create_impala_metric_struct("statestore_subscriber_last_recovery_duration", "The amount of time, in seconds, the StateStore subscriber took to recover the connection the last time it was lost.")
  impala_tcmalloc_free_bytes =                 create_impala_metric_struct("tcmalloc_pageheap_free_bytes", "Number of bytes in free, mapped pages in page heap. These bytes can be used to fulfill allocation requests. They always count towards virtual memory usage, and unless the underlying memory is swapped out by the OS, they also count towards physical memory usage.")
  impala_tcmalloc_physical_reserved_bytes =    create_impala_metric_struct("tcmalloc_physical_bytes_reserved", "Derived metric computing the amount of physical memory (in bytes) used by the process, including that actually in use and free bytes reserved by tcmalloc. Does not include the tcmalloc metadata.")
  impala_tcmalloc_total_reserved_bytes =       create_impala_metric_struct("tcmalloc_total_bytes_reserved", "Bytes of system memory reserved by TCMalloc.")
  impala_tcmalloc_unmapped_bytes =             create_impala_metric_struct("tcmalloc_pageheap_unmapped_bytes", "Number of bytes in free, unmapped pages in page heap. These are bytes that have been released back to the OS, possibly by one of the MallocExtension \"Release\" calls. They can be used to fulfill allocation requests, but typically incur a page fault. They always count towards virtual memory usage, and depending on the OS, typically do not count towards physical memory usage.")
  impala_tcmalloc_used_bytes =                 create_impala_metric_struct("tcmalloc_bytes_in_use", "Number of bytes used by the application. This will not typically match the memory use reported by the OS, because it does not include TCMalloc overhead or memory fragmentation.")
  impala_thrift_connections_rate =             create_impala_metric_struct("thrift_server_catalog_service_connections_rate", "The total number of connections made to this Catalog Server's catalog service over its lifetime.")
  impala_thrift_connections_used =             create_impala_metric_struct("thrift_server_catalog_service_connections_in_use", "The number of active catalog service connections to this Catalog Server.")
  impala_write_rate =                          create_impala_metric_struct("write_bytes_rate", "The number of bytes written to the device.")

)
var impala_query_variable_relationship = []relationa {
  {&IMPALA_CATALOG_JVM_COMITTED_BYTES,          *impala_catalog_jvm_comitted_bytes},
  {&IMPALA_CATALOG_JVM_CURRENT_BYTES,           *impala_catalog_jvm_current_bytes},
  {&IMPALA_CATALOG_JVM_INIT_BYTES,              *impala_catalog_jvm_init_bytes},
  {&IMPALA_CATALOG_JVM_MAX_BYTES,               *impala_catalog_jvm_max_bytes},
  {&IMPALA_CGROUP_MEM_PAGE_CACHE,               *impala_cgroup_mem_page_cache},
  {&IMPALA_CGROUP_MEM_RSS,                      *impala_cgroup_mem_rss},
  {&IMPALA_CGROUP_MEM_SWAP,                     *impala_cgroup_mem_swap},
  {&IMPALA_CGROUP_READ_IOSRATE,                 *impala_cgroup_read_iosrate},
  {&IMPALA_CGROUP_READ_RATE,                    *impala_cgroup_read_rate},
  {&IMPALA_CGROUP_SYSTEM_RATE,                  *impala_cgroup_system_rate},
  {&IMPALA_CGROUP_USER_RATE,                    *impala_cgroup_user_rate},
  {&IMPALA_CGROUP_WRITE_IOSRATE,                *impala_cgroup_write_iosrate},
  {&IMPALA_CGROUP_WRITE_RATE,                   *impala_cgroup_write_rate},
  {&IMPALA_MEM_RSS,                             *impala_mem_rss},
  {&IMPALA_MEM_SWAP,                            *impala_mem_swap},
  {&IMPALA_MEM_VIRT,                            *impala_mem_virt},
  {&IMPALA_OOMEXIT,                             *impala_oomexit},
  {&IMPALA_QUERY_ADMISSION_WAIT_RATE,           *impala_query_admission_wait_rate},
  {&IMPALA_QUERY_BYTES_HDFS_READ_RATE,          *impala_query_bytes_hdfs_read_rate},
  {&IMPALA_QUERY_BYTES_HDFS_WRITTE_RATE,        *impala_query_bytes_hdfs_writte_rate},
  {&IMPALA_QUERY_BYTES_STREAMED_RATE,           *impala_query_bytes_streamed_rate},
  {&IMPALA_QUERY_CM_CPU,                        *impala_query_cm_cpu},
  {&IMPALA_QUERY_DURATION_RATE,                 *impala_query_duration_rate},
  {&IMPALA_QUERY_INGESTED_RATE,                 *impala_query_ingested_rate},
  {&IMPALA_QUERY_MEM_ACCRUAL_RATE,              *impala_query_mem_accrual_rate},
  {&IMPALA_QUERY_MEM_SPILLED_RATE,              *impala_query_mem_spilled_rate},
  {&IMPALA_QUERY_OOMRATE,                       *impala_query_oomrate},
  {&IMPALA_QUERY_REJECTED_RATE,                 *impala_query_rejected_rate},
  {&IMPALA_QUERY_SPILLED_RATE,                  *impala_query_spilled_rate},
  {&IMPALA_QUERY_SUCCESSFUL_RATE,               *impala_query_successful_rate},
  {&IMPALA_QUERY_THREAD_CPU_RATE,               *impala_query_thread_cpu_rate},
  {&IMPALA_QUERY_TIME_OUT_RATE,                 *impala_query_time_out_rate},
  {&IMPALA_READ_RATE,                           *impala_read_rate},
  {&IMPALA_STATE_STORE_CACHE_TOTAL_CLIENTS,     *impala_state_store_cache_total_clients},
  {&IMPALA_STATE_STORE_CLIENTS_IN_USE,          *impala_state_store_clients_in_use},
  {&IMPALA_STATE_STORE_HEART_BEAT_LAST,         *impala_state_store_heart_beat_last},
  {&IMPALA_STATE_STORE_HEART_BEAT_MAX,          *impala_state_store_heart_beat_max},
  {&IMPALA_STATE_STORE_HEART_BEAT_MEAN,         *impala_state_store_heart_beat_mean},
  {&IMPALA_STATE_STORE_HEART_BEAT_MIN,          *impala_state_store_heart_beat_min},
  {&IMPALA_STATE_STORE_HEART_BEAT_RATE,         *impala_state_store_heart_beat_rate},
  {&IMPALA_STATE_STORE_HEART_BEAT_STDDEV,       *impala_state_store_heart_beat_stddev},
  {&IMPALA_STATE_STORE_LAST_RECOVERY_DURATION,  *impala_state_store_last_recovery_duration},
  {&IMPALA_TCMALLOC_FREE_BYTES,                 *impala_tcmalloc_free_bytes},
  {&IMPALA_TCMALLOC_PHYSICAL_RESERVED_BYTES,    *impala_tcmalloc_physical_reserved_bytes},
  {&IMPALA_TCMALLOC_TOTAL_RESERVED_BYTES,       *impala_tcmalloc_total_reserved_bytes},
  {&IMPALA_TCMALLOC_UNMAPPED_BYTES,             *impala_tcmalloc_unmapped_bytes},
  {&IMPALA_TCMALLOC_USED_BYTES,                 *impala_tcmalloc_used_bytes},
  {&IMPALA_THRIFT_CONNECTIONS_RATE,             *impala_thrift_connections_rate},
  {&IMPALA_THRIFT_CONNECTIONS_USED,             *impala_thrift_connections_used},
  {&IMPALA_WRITE_RATE,                          *impala_write_rate},
}




/* ======================================================================
 * Functions
 * ====================================================================== */
// Create and returns a prometheus descriptor for a impala metric. 
// The "metric_name" parameter its mandatory
// If the "description" parameter is empty, the function assings it with the
// value of the name of the metric in uppercase and separated by spaces
func create_impala_metric_struct(metric_name string, description string) *prometheus.Desc {
  // Correct "description" parameter if is empty
  if len(description) == 0 {
    description = strings.Replace(strings.ToUpper(metric_name), "_", " ", 0)
  }

  // return prometheus descriptor
  return prometheus.NewDesc(
    prometheus.BuildFQName(namespace, IMPALA_SCRAPER_NAME, metric_name),
    description,
    []string{"cluster", "entityName"},
    nil,
  )
}


// Generic function to extract de metadata associated with the query value
// Only for Impala metric type
func create_impala_metric (ctx context.Context, config Collector_connection_data, query string, metric_struct prometheus.Desc, ch chan<- prometheus.Metric) bool {
  if query == "" { return true }
  // Make the query
  json_parsed, err := make_and_parse_timeseries_query(ctx, config, query)
  if err != nil {
    return false
  }

  // Get the num of hosts in the cluster or clusters
  num_ts_series, err := jp.Get_timeseries_num(json_parsed)
  if err != nil {
    return false
  }

  // Extract Metadata for each TimeSerie
  for ts_index := 0; ts_index < num_ts_series; ts_index ++ {
    // Get the Cluster Name
    cluster_name := jp.Get_timeseries_query_cluster(json_parsed, ts_index)
    entity_name := jp.Get_timeseries_query_entity_name(json_parsed, ts_index)
    // Get Query LAST value
    value, err := jp.Get_timeseries_query_value(json_parsed, ts_index)
    if err != nil {
      log.Debug_msg("No data for query: %s", query)
      continue
    }
    // Assing the data to the Prometheus descriptor
    ch <- prometheus.MustNewConstMetric(&metric_struct, prometheus.GaugeValue, value, cluster_name, entity_name)
  }
  return true
}




/* ======================================================================
 * Scrape "Class"
 * ====================================================================== */
// ScrapeImpala struct
type ScrapeImpalaMetrics struct{}

// Name of the Scraper. Should be unique.
func (ScrapeImpalaMetrics) Name() string {
  return IMPALA_SCRAPER_NAME
}

// Help describes the role of the Scraper.
func (ScrapeImpalaMetrics) Help() string {
    return "Collect Impala Service Metrics"
}

// Version.
func (ScrapeImpalaMetrics) Version() float64 {
    return 1.0
}

func (ScrapeImpalaMetrics) Scrape(ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error {
  log.Debug_msg("Ejecutando Hosts Metrics Scraper")

  // Queries counters
  success_queries := 0
  error_queries := 0

  // Get Cloudera Version
  cm_version := get_cloudera_manager_version(ctx, *config)
  log.Debug_msg("Version Cloudera: %s", cm_version)
  load_impala_queries(cm_version)


  // Execute the generic funtion for creation of metrics with the pairs (QUERY, PROM:DESCRIPTOR)
  for i:=0 ; i < len(impala_query_variable_relationship) ; i++ {
    if create_impala_metric(ctx, *config, *impala_query_variable_relationship[i].Query, impala_query_variable_relationship[i].Metric_struct, ch) {
      success_queries += 1
    } else {
      error_queries += 1
    }
  }
  log.Debug_msg("In the Impala Module has been executed %d queries. %d success and %d with errors", success_queries + error_queries, success_queries, error_queries)
  return nil
}


