/*
 *
 * title           :config_parser
 * description     :Module to read and check the cloudera exporter config file 
 * author		       :Alejandro Villegas
 * date            :2019/01/31
 *
 */
package config_parser


/* ======================================================================
 * Dependencies and libraries
 * ====================================================================== */
import (
  // Own Libraries
  cl "keedio/cloudera_exporter/collector"
  log "keedio/cloudera_exporter/logger"
  "errors"

  // Go External libraries
  "gopkg.in/ini.v1"
)




/* ======================================================================
 * Error Messages
 * ====================================================================== */
 var (
  error_msg_no_user =     "No user specified in config file"
  error_msg_no_password = "No password specified in config file"
  error_msg_no_host =     "No host specified in config file"
  error_msg_no_port =     "No port specified in config file"
  error_msg_no_num_procs = "No num_procs specified in config file"
  error_msg_no_deploy_ip = "No deploy_ip specified in config file. The exporter will use the public IP"
  error_msg_no_deploy_port = "No deploy_port specified in config file"
  error_msg_no_log_level = "No log_level specified in config file"
)




/* ======================================================================
 * Data Structs
 * ====================================================================== */
// Struct to store the list of Scrapers and if they are going to be loaded
type CE_collectors_flags struct {
  Scrapers map [cl.Scraper] bool
}

// Struct to group the two previous structs and some exporter configuration parameters
type CE_config struct {
  Num_procs int
  Connection cl.Collector_connection_data
  Scrapers CE_collectors_flags
  Deploy_ip string
  Deploy_port uint
  Log_level int
}




/* ======================================================================
 * Functions
 * ====================================================================== */
func parse_user (config_reader *ini.File) (string, error) {
  user := config_reader.Section("user").Key("username").String()
  if user == "" {
    log.Err_msg(error_msg_no_user)
    return "", errors.New(error_msg_no_user)
  }
  return user, nil
}


func parse_passwd (config_reader *ini.File) (string, error) {
  password := config_reader.Section("user").Key("password").String()
  if password == "" {
    log.Err_msg(error_msg_no_password)
    return "", errors.New(error_msg_no_password)
  }
  return password, nil
}


func parse_host (config_reader *ini.File) (string, error) {
  host := config_reader.Section("target").Key("host").String()
  if host == "" {
    log.Err_msg(error_msg_no_host)
    return "", errors.New(error_msg_no_host)
  }
  return host, nil
}


func parse_port (config_reader *ini.File) (string, error) {
  port := config_reader.Section("target").Key("port").String()
  if port == "" {
    log.Err_msg(error_msg_no_port)
    return "", errors.New(error_msg_no_port)
  }
  return port, nil
}

func parse_use_tls(config_reader *ini.File) bool {
  use_tls_flag := config_reader.Section("target").Key("use_tls").MustBool(false)
  return use_tls_flag
}

func parse_api_version (config_reader *ini.File) (string, error) {
  api_version := config_reader.Section("target").Key("version").String()
  if api_version == "" {
    return "", nil
  }
  log.Warn_msg("Overwritting API Version value: %s", api_version)
  return api_version, nil
}



// Dynamic load of modules
func parse_global_status_module_flag (config_reader *ini.File) bool {
  global_status_module_flag := config_reader.Section("modules").Key("global_status_module").MustBool(false)
  return global_status_module_flag
}

func parse_host_module_flag (config_reader *ini.File) bool {
  host_module_flag := config_reader.Section("modules").Key("host_module").MustBool(false)
  return host_module_flag
}

func parse_impala_module_flag (config_reader *ini.File) bool {
  impala_module_flag := config_reader.Section("modules").Key("impala_module").MustBool(false)
  return impala_module_flag
}

func parse_hdfs_module_flag (config_reader *ini.File) bool {
  hdfs_module_flag := config_reader.Section("modules").Key("hdfs_module").MustBool(false)
  return hdfs_module_flag
}

func parse_yarn_module_flag (config_reader *ini.File) bool {
  yarn_module_flag := config_reader.Section("modules").Key("yarn_module").MustBool(false)
  return yarn_module_flag
}


func parse_num_procs (config_reader *ini.File) (int, error) {
  num_procs := config_reader.Section("system").Key("num_procs").MustInt(0)
  if num_procs == 0 {
    log.Err_msg(error_msg_no_num_procs)
    return 0, errors.New(error_msg_no_num_procs)
  }
  return num_procs, nil
}

func parse_deploy_ip (config_reader *ini.File) (string, error) {
  deploy_ip := config_reader.Section("system").Key("deploy_ip").String()
  if deploy_ip == "" {
    log.Warn_msg(error_msg_no_deploy_ip)
    return "", errors.New(error_msg_no_deploy_ip)
  }
  return deploy_ip, nil
}

func parse_deploy_port (config_reader *ini.File) (uint, error) {
  deploy_port := config_reader.Section("system").Key("deploy_port").MustUint(0)
  if deploy_port == 0 {
    log.Err_msg(error_msg_no_deploy_port)
    return 0, errors.New(error_msg_no_deploy_port)
  }
  return deploy_port, nil
}

func parse_log_level (config_reader *ini.File) (int, error) {
  log_level := config_reader.Section("system").Key("log_level").MustInt(-1)
  if log_level == -1 {
    log.Err_msg(error_msg_no_log_level)
    return 0, errors.New(error_msg_no_log_level)
  }
  return log_level, nil
}


func Parse_config(config interface{}) (*CE_config, error) {
  var err error

  opts := ini.LoadOptions {
    AllowBooleanKeys: true, // Config file can have boolean keys.
  }
  cfg, err := ini.LoadSources(opts, config)
  if err != nil {
    log.Err_msg("Failed reading config file: %s", err)
    return nil, err
  }

  // Parse File Options

  // Username
  user, err := parse_user(cfg)
  if err != nil {
    log.Err_msg("Can't parse user field")
    return nil, err
  }

  // Password
  password, err := parse_passwd(cfg)
  if err != nil {
    log.Err_msg("Can't parse password field")
    return nil, err
  }

  // Cloudera Manager entrypoint
  host, err := parse_host(cfg)
  if err != nil {
    log.Err_msg("Can't parse host field")
    return nil, err
  }

  // Cloudera Manager Port
  port, err := parse_port(cfg)
  if err != nil {
    log.Err_msg("Can't parse port field")
    return nil, err
  }

  // Cloudera Manager API Version
  api_version, err := parse_api_version(cfg)
  if err != nil {
    log.Err_msg("Can't parse api_version field")
    return nil, err
  }

  global_status_module_flag := parse_global_status_module_flag (cfg)
  use_tls_flag := parse_use_tls (cfg)
  host_module_flag := parse_host_module_flag (cfg)
  impala_module_flag := parse_impala_module_flag (cfg)
  hdfs_module_flag := parse_hdfs_module_flag (cfg)
  yarn_module_flag := parse_yarn_module_flag (cfg)


  // System parameters
  num_procs, err := parse_num_procs(cfg)
  if err != nil && err.Error() != error_msg_no_num_procs {
    log.Err_msg("Can't parse num_procs field")
    return nil, err
  }
  deploy_ip, err := parse_deploy_ip(cfg)
  if err != nil && err.Error() != error_msg_no_deploy_ip{
    log.Err_msg("Can't parse deploy_ip field")
    return nil, err
  }
  deploy_port, err := parse_deploy_port(cfg)
  if err != nil && err.Error() != error_msg_no_deploy_port {
    log.Err_msg("Can't parse deploy_port field")
    return nil, err
  }
  log_level, err := parse_log_level(cfg)
  if err != nil && err.Error() != error_msg_no_log_level {
    log.Err_msg("Can't parse log_level field")
    return nil, err
  }


  return &CE_config {
    num_procs,
    cl.Collector_connection_data {
      Host:        host,
      Port:        port,
      UseTls:      use_tls_flag,
      Api_version: api_version,
      User:        user,
      Passwd:      password,
    },
    CE_collectors_flags{
      map [cl.Scraper] bool {
        cl.ScrapeStatus{}: global_status_module_flag,
        cl.ScrapeHost{}: host_module_flag,
        cl.ScrapeImpalaMetrics{}: impala_module_flag,
        cl.ScrapeHDFS{}: hdfs_module_flag,
        cl.ScrapeYARNMetrics{}: yarn_module_flag,
      },
    },
  deploy_ip,
  deploy_port,
  log_level,
  },
  nil
}
