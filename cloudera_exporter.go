/*
 *
 * title           :cloudera_exporter.go
 * description     :Main code file of the Cloudera Exporter for Prometheus DB
 * author		       :Raul Barroso and Alejandro Villegas
 * date            :2018/10/05
 *
 */
package main




/* ======================================================================
 * Dependencies and libraries
 * ====================================================================== */
import (
  "context"
  "fmt"
  // Go Default libraries
  "net/http"
  "os"
  "path"
  "runtime"
  "strconv"
  "strings"
  "time"

  // Own libraries
  cl "keedio/cloudera_exporter/collector"
  cp "keedio/cloudera_exporter/config_parser"
  log "keedio/cloudera_exporter/logger"

  // Go external libraries
  "gopkg.in/alecthomas/kingpin.v2"

  "github.com/prometheus/client_golang/prometheus"
  "github.com/prometheus/client_golang/prometheus/promhttp"
  // Go Prometheus libraries
  "github.com/prometheus/common/version"
)




/* ======================================================================
 * Global variables
 * ====================================================================== */
 // Exporter Configuration Struct
var config *cp.CE_config

// Timeout Offset for Prometheus TimeStamping
var timeoutOffset = 0.0

// HTML Code por Landing Page
var metrics_path="/metrics"
  var landingPage = []byte(`<html>
  <head><title>Cloudera Manager exporter</title></head>
  <body>
  <h1>Cloudera Manager exporter</h1>
  <h3> by KEEDIO - Big Data Facilitators</h3>
  <p><a href='` + metrics_path + `'>Metrics</a></p>
  </body>
  </html>
`)


/* ======================================================================
 * Functions
 * ====================================================================== */
// Creates and initialize a Prometheus Collector
func init() {
  set_version_properties()
	prometheus.MustRegister(version.NewCollector("kbdi"))
}


// Create and returns a Handler for the Collector
func newHandler(metrics cl.Metrics, scrapers []cl.Scraper) http.HandlerFunc {
  return func(w http.ResponseWriter, r *http.Request) {

    // Use request context for cancellation when connection gets closed.
    ctx := r.Context()

    // If a timeout is configured via the Prometheus header, add it to the context.
    if v := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); v != "" {
      timeoutSeconds, err := strconv.ParseFloat(v, 64)
      if err != nil {
          log.Err_msg("Failed to parse timeout from Prometheus header: %s", err.Error())
      } else {
        if timeoutOffset >= timeoutSeconds {
          // Ignore timeout offset if it doesn't leave time to scrape.
          log.Err_msg("Timeout offset (--timeout-offset=%.2f) should be lower than prometheus scrape time (X-Prometheus-Scrape-Timeout-Seconds=%.2f).", timeoutOffset, timeoutSeconds)
        } else {
          // Subtract timeout offset from timeout.
          timeoutSeconds -= timeoutOffset
        }

        // Create new timeout context with request context as parent.
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSeconds * float64(time.Second)))
        defer cancel()

        // Overwrite request with timeout context.
        r = r.WithContext(ctx)
      }
    }

    // Create Prometheus registry with filtererd scrapers
    registry := prometheus.NewRegistry()

    // Register the collector with the data connection struct in the registry
    registry.MustRegister(cl.New(ctx, config.Connection, metrics, scrapers))

    gatherers := prometheus.Gatherers { prometheus.DefaultGatherer, registry }

    // Delegate http serving to Prometheus client library, which will call collector.Collect.
    h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
    h.ServeHTTP(w, r)
  }
}


// Set the version properties of the Cloudera Exporter
func set_version_properties() {
  version.Version="1.3"
  version.Revision="PRO"
  version.Branch="Master"
  version.BuildUser="Keedio"
  currentTime := time.Now()
  version.BuildDate=currentTime.String()
}


// Prepare and parse the execution flags
func parse_exec_flags () {
  kingpin.Version(version.Print("cloudera_exporter"))
  kingpin.HelpFlag.Short('h')
  kingpin.Parse()
}


// Register scrapers enabled.
func register_scrapers (config *cp.CE_config) []cl.Scraper{
  enabledScrapers := []cl.Scraper{}
  log.Info_msg("Enabled scrapers:")
  for scraper, enabled := range config.Scrapers.Scrapers {
    if enabled {
      log.Info_msg(" -> %s", strings.Title(strings.Replace(scraper.Name(), "_", " ", -1)))
      enabledScrapers = append(enabledScrapers, scraper)
    }
  }
  return enabledScrapers
}


// Read the flags and the config file and set all the values of the
// Configuration Structure
func parse_flags_and_config_file() error {
  var err error

  // Parse flags and config file
  configFile := kingpin.Flag("config-file", "Path to ini file.", ).Default(path.Join(os.Getenv("HOME"), "config.ini")).String()
  arg_host := *(kingpin.Flag("web.listen-address", "Listent Address.",).Default("").String())
  arg_num_procs := *(kingpin.Flag("num-procs", "Number Processes for parallel execution",).Default("0").Int())
  arg_log_level := *(kingpin.Flag("log-level", "Debug Log Mode",).Default("0").Int())
  timeoutOffset = *(kingpin.Flag("timeout-offset", "Time to subtract from timeout in seconds.", ).Default("0.25").Float64())
  parse_exec_flags()

  if config, err = cp.Parse_config(*configFile); err != nil {
    return err
  }

  // If host, num_procs or log_level are defined in the execution flags, they
  // have priority over the configuration file
  if arg_host != "" {
    config.Connection.Host = arg_host
  }
  if arg_num_procs != 0 {
    config.Num_procs = arg_num_procs
  }
  if arg_log_level != 0 {
    config.Log_level = arg_log_level
  }


  // Check if Api_version is defined on the config file, else, the version is
  // obtained by Cloudera Manager API
  if config.Connection.Api_version == "" {
    if config.Connection.Api_version, err = cl.Get_api_cloudera_version(nil, config.Connection); err != nil {
      return err
    }
  }
  cl.SendConf(config)
  return nil
}

// Main function
func main(){
  // Starting Logging
  log.Init(os.Stdout, os.Stdout, os.Stdout, os.Stderr, os.Stdout, 0)
  log.Info_msg("================================================================================")
  log.Info_msg("Starting Keedio Cloudera's Metrics Exporter")

  // Setting code version properties
  log.Info_msg("Exporter Version: %s", version.Version)

  // Parse Flags and config file
  if err := parse_flags_and_config_file(); err != nil {
    log.Err_msg(err.Error())
    return
  }
  log.Init(os.Stdout, os.Stdout, os.Stdout, os.Stderr, os.Stdout, config.Log_level)

  //Parallel Execution
  runtime.GOMAXPROCS(config.Num_procs)
  log.Info_msg("Cores allocated: %s", strconv.Itoa(config.Num_procs))

  // Run info
  log.Info_msg("Build context %s", version.BuildContext())

  // Exporter creation
  log.Info_msg("Registering Handlers")
  handlerFunc := newHandler(cl.NewMetrics(), register_scrapers(config))
  http.Handle(metrics_path, promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, handlerFunc))
  http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.Write(landingPage) })
  log.Ok_msg("Landing Page and Handlers are running")


  // Exporter HTTP connection
  log.Info_msg("Target to scraping metrics from: %s:%s", config.Connection.Host, config.Connection.Port)
  ip := func () string {if config.Deploy_ip == "" { return "0.0.0.0" } else { return config.Deploy_ip }}
  log.Info_msg("Metrics published on: %s:%d", ip(), config.Deploy_port)
  log.Ok_msg("Keedio's Cloudera Exporter running")
  log.Err_msg(http.ListenAndServe(fmt.Sprintf("%s:%d", config.Deploy_ip, config.Deploy_port), nil).Error())
  return
}
