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



import (
  log "keedio/cloudera_exporter/logger"
)


/* ======================================================================
 * Future improvements
 * ====================================================================== */
//https://golangvedu.wordpress.com/2017/01/31/golang-design-pattern-abstract-factory-and-factory-method/


func load_impala_queries(version string) {
  log.Debug_msg("Setting queries to Cloudera Manager version %s", version)
  switch version {
  case "5.16.1":
    IMPALA_QUERY_ADMISSION_WAIT_RATE =            ""
    IMPALA_QUERY_BYTES_HDFS_READ_RATE =           ""
    IMPALA_QUERY_BYTES_HDFS_WRITTE_RATE =         ""
    IMPALA_QUERY_BYTES_STREAMED_RATE =            ""
    IMPALA_QUERY_CM_CPU =                         ""
    IMPALA_QUERY_DURATION_RATE =                  ""
    IMPALA_QUERY_MEM_ACCRUAL_RATE =               ""
    IMPALA_QUERY_MEM_SPILLED_RATE =               "SELECT LAST(INTEGRAL(queries_spilled_memory_rate)) WHERE entityName rlike \".*impala.*\""
    IMPALA_QUERY_THREAD_CPU_RATE =                ""
  case "5.8":
    IMPALA_CATALOG_JVM_COMITTED_BYTES =           ""
    IMPALA_CATALOG_JVM_CURRENT_BYTES =            ""
    IMPALA_CATALOG_JVM_INIT_BYTES =               ""
    IMPALA_CATALOG_JVM_MAX_BYTES =                ""

    default:
    log.Warn_msg("Dont Have specific queries for Cloudera Manager version %s", version)
  }
}


