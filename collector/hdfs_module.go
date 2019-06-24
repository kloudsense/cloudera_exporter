/*
 *
 * title           :collector/hdfs_module.go
 * description     :Submodule Collector for the Cluster HDFS metrics
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
const HDFS_SCRAPER_NAME = "hdfs"
const (
  // Agent Queries
    HDFS_DFS_CAPACITY=                 "SELECT LAST(dfs_capacity) WHERE category=SERVICE"
    HDFS_DFS_CAPACITY_USED =           "SELECT LAST(dfs_capacity_used) WHERE category=SERVICE"
    HDFS_DFS_CAPACITY_USED_PERCENT =   "SELECT LAST(((100 * dfs_capacity_used) / dfs_capacity)) WHERE serviceType=HDFS AND category=SERVICE and entityName rlike \"hdfs:.*\""
    HDFS_DFS_CAPACITY_NON_HDFS_USED =  "SELECT LAST(dfs_capacity_used_non_hdfs) WHERE category=SERVICE"
    HDFS_BLOCK_CAPACITY =              "SELECT LAST(block_capacity) WHERE category=SERVICE"
    HDFS_BLOCK_TOTAL =                 "SELECT LAST(blocks_total) WHERE category=SERVICE"
    HDFS_BLOCK_CORRUPT_REPLICAS =      "SELECT LAST(blocks_with_corrupt_replicas) WHERE category=SERVICE"
    HDFS_BLOCK_EXCESS =                "SELECT LAST(excess_blocks) WHERE category=SERVICE"
    HDFS_BLOCK_MISSING =               "SELECT LAST(missing_blocks) WHERE category=SERVICE"
    HDFS_BLOCK_UNDER_REPLICATED =      "SELECT LAST(under_replicated_blocks) WHERE category=SERVICE"
    HDFS_BLOCK_WRITE =                 "SELECT LAST(INTEGRAL(total_blocks_written_rate_across_datanodes)) WHERE entityName=hdfs"
    HDFS_BLOCK_READ =                  "SELECT LAST(INTEGRAL(total_blocks_read_rate_across_datanodes)) WHERE entityName=hdfs"
    HDFS_FILES_TOTAL =                 "SELECT LAST(files_total) WHERE category=SERVICE"
    HDFS_FILES_SIZE_AVG =              "SELECT LAST(dfs_capacity_used / files_total) WHERE category=SERVICE"
    HDFS_HEARTBEATS_EXPIRED =          "SELECT LAST(expired_heartbeats) WHERE category=SERVICE"
    HDFS_NAMENODE_FD_MAX_DESCRIPTORS = "SELECT LAST(fd_max_across_namenodes) WHERE category=SERVICE"
    HDFS_SNAPSHOT_NUM =                "SELECT LAST(total_snapshots_across_namenodes) WHERE category=CLUSTER and entityName=1"
    HDFS_SNAPSHOT_DIRS =               "SELECT LAST(total_snapshottable_directories_across_namenodes) WHERE category=CLUSTER and entityName=1"
)




/* ======================================================================
 * Global variables
 * ====================================================================== */
// Prometheus data Descriptors for the metrics to export
var (
  // Agent Metrics
  hdfs_dfs_capacity =                create_hdfs_metric_struct("dfs_capacity", "Distributed File System Capacity")
  hdfs_dfs_capacity_used =           create_hdfs_metric_struct("dfs_capacity_used", "Distributed File System Capacity Used")
  hdfs_dfs_capacity_used_percent =   create_hdfs_metric_struct("dfs_capacity_used_in_percent",  "Distributed File System Capacity Used in X Percent")
  hdfs_dfs_capacity_non_hdfs_used =  create_hdfs_metric_struct("dfs_capacity_non_hdfs_used", "Distributed File System Capacity Used by Non HDFS File System")
  hdfs_block_capacity =              create_hdfs_metric_struct("block_capacity", "Distributed File System Num Blocks Capacity")
  hdfs_block_total =                 create_hdfs_metric_struct("block_total", "Distributed File System Num Blocks Total")
  hdfs_block_corrupt_replicas =      create_hdfs_metric_struct("block_corrupt_replicas", "Distributed File System Num Block with corrupted replicas")
  hdfs_block_excess =                create_hdfs_metric_struct("block_excess", "Distributed File System Num Excess blocks")
  hdfs_block_missing =               create_hdfs_metric_struct("block_missing", "Distributed File System Num Missing blocks")
  hdfs_block_under_replicated =      create_hdfs_metric_struct("block_under_replicated", "Distributed File System Num Under-Replicated blocks")
  hdfs_block_write =                 create_hdfs_metric_struct("block_write_rate", "Distributed File System Rate Writed blocks")
  hdfs_block_read =                  create_hdfs_metric_struct("block_read_rate", "Distributed File System Rate Readed blocks")
  hdfs_files_total =                 create_hdfs_metric_struct("files_total", "Distributed File System Num Total Files In HDFS")
  hdfs_files_size_avg =              create_hdfs_metric_struct("files_average_size", "Distributed File System Avg Size of Files In HDFS")
  hdfs_heartbeats_expired =          create_hdfs_metric_struct("heartbeat_expired", "Distributed File System Num Total Heartbeats Expired")
  hdfs_namenode_fd_max_descriptors = create_hdfs_metric_struct("namenode_fd_max_descriptors", "Distributed File System Namenode Max File Descriptors")
  hdfs_snapshot_num =                create_hdfs_metric_struct("snapshot_num",  "Distributed File System Num Total Snapshots")
  hdfs_snapshot_dirs=                create_hdfs_metric_struct("snapshot_dirs",  "Distributed File System Num Total Snapshottable Dirs")

)

// Creation of the structure that relates the queries with the descriptors of the Prometheus metrics
var hdfs_query_variable_relationship = []relation {
  {HDFS_DFS_CAPACITY,                *hdfs_dfs_capacity},
  {HDFS_DFS_CAPACITY_USED,           *hdfs_dfs_capacity_used},
  {HDFS_DFS_CAPACITY_USED_PERCENT,   *hdfs_dfs_capacity_used_percent},
  {HDFS_DFS_CAPACITY_NON_HDFS_USED,  *hdfs_dfs_capacity_non_hdfs_used},
  {HDFS_BLOCK_CAPACITY,              *hdfs_block_capacity},
  {HDFS_BLOCK_TOTAL,                 *hdfs_block_total},
  {HDFS_BLOCK_CORRUPT_REPLICAS,      *hdfs_block_corrupt_replicas},
  {HDFS_BLOCK_EXCESS,                *hdfs_block_excess},
  {HDFS_BLOCK_MISSING,               *hdfs_block_missing},
  {HDFS_BLOCK_UNDER_REPLICATED,      *hdfs_block_under_replicated},
  {HDFS_BLOCK_WRITE,                 *hdfs_block_write},
  {HDFS_BLOCK_READ,                  *hdfs_block_read},
  {HDFS_FILES_TOTAL,                 *hdfs_files_total},
  {HDFS_FILES_SIZE_AVG,              *hdfs_files_size_avg},
  {HDFS_HEARTBEATS_EXPIRED,          *hdfs_heartbeats_expired},
  {HDFS_NAMENODE_FD_MAX_DESCRIPTORS, *hdfs_namenode_fd_max_descriptors},
  {HDFS_SNAPSHOT_NUM,                *hdfs_snapshot_num},
  {HDFS_SNAPSHOT_DIRS,               *hdfs_snapshot_dirs},
}




/* ======================================================================
 * Functions
 * ====================================================================== */
// Create and returns a prometheus descriptor for a hdfs metric. 
// The "metric_name" parameter its mandatory
// If the "description" parameter is empty, the function assings it with the
// value of the name of the metric in uppercase and separated by spaces
func create_hdfs_metric_struct(metric_name string, description string) *prometheus.Desc {
  // Correct "description" parameter if is empty
  if len(description) == 0 {
    description = strings.Replace(strings.ToUpper(metric_name), "_", " ", 0)
  }

  // return prometheus descriptor
  return prometheus.NewDesc(
    prometheus.BuildFQName(namespace, HDFS_SCRAPER_NAME, metric_name),
    description,
    []string{"cluster", "entityName"},
    nil,
  )
}


// Generic function to extract de metadata associated with the query value
// Only for HDFS metric type
func create_hdfs_metric (ctx context.Context, config Collector_connection_data, query string, metric_struct prometheus.Desc, ch chan<- prometheus.Metric) bool {
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
    // Get the entity Name
    entity_name := jp.Get_timeseries_query_entity_name(json_parsed, ts_index)
    // Get Query LAST value
    value, err := jp.Get_timeseries_query_value(json_parsed, ts_index)
    if err != nil {
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
// ScrapeHDFS struct
type ScrapeHDFS struct{}

// Name of the Scraper. Should be unique.
func (ScrapeHDFS) Name() string {
  return HDFS_SCRAPER_NAME
}

// Help describes the role of the Scraper.
func (ScrapeHDFS) Help() string {
  return "HDFS Metrics"
}

// Version.
func (ScrapeHDFS) Version() float64 {
  return 1.0
}

// Scrape generic function. Override for host module.
func (ScrapeHDFS) Scrape (ctx context.Context, config *Collector_connection_data, ch chan<- prometheus.Metric) error {
  log.Debug_msg("Ejecutando HDFS Metrics Scraper")

  // Queries counters
  success_queries := 0
  error_queries := 0

  // Execute the generic funtion for creation of metrics with the pairs (QUERY, PROM:DESCRIPTOR)
  for i:=0 ; i < len(hdfs_query_variable_relationship) ; i++ {
    if create_hdfs_metric(ctx, *config, hdfs_query_variable_relationship[i].Query, hdfs_query_variable_relationship[i].Metric_struct, ch) {
      success_queries += 1
    } else {
      error_queries += 1
    }
  }
  log.Debug_msg("In the HDFS Module has been executed %d queries. %d success and %d with errors", success_queries + error_queries, success_queries, error_queries)
  return nil
}

var _ Scraper = ScrapeHDFS{}
