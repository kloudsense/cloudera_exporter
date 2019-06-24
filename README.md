# Keedio Cloudera Exporter ![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg?style=plastic)

![GO](https://img.shields.io/badge/go_version-1.12-blue.svg?style=plastic) ![Docker](https://img.shields.io/badge/docker_container-ready-brightgreen.svg?style=plastic)




## What is it?
This project builds a Prometheus Exporter to collect Cloudera status and usage metrics.




### Dependencies
The dependencies are:
* _GoLang >= v1.12_
* _github.com/prometheus/client_golang/prometheus_
* _github.com/tidwall/gjson gopkg.in/alecthomas/kingpin.v2_
* _github.com/sirupsen/logrus gopkg.in/ini.v1_




## Modules
This exporter scrape the metrics by independent modules (Scrapers). This modules are:
* **Status:**  Scrapes the metrics about the current status of the Clusters, services, roles and hosts
* **Hosts:**  Scrapes the metrics about the Hosts: CPU usage, RAM, SWAP, Agent stats and more useful metrics
* **HDFS:**  Scrapes the metrics about HDFS: Capacity, blocks stats, file stats, Namenode properties and Snapshots.
* **Impala:**  Scrapes the metrics about Impala: Catalog, usage stats, queries stats, state-store info …




## Building and Running
To launch the exporter we recommend use a Docker container.  Whether it is inside a container or in the local system, some Golang packages are needed as dependencies of the code to be able to implement some Prometheus functions.




### Docker Deploy
#### Configuration
Edit the *config.ini* file and change the host ip (cloudera_manager) in the *target* section by your Cloudera Manager IP addr.  If you want to use "cloudera_manager" as a name-domain, edit the *Makefile.common* file and change "--add-host" property in the *Docker_RUN_FLAGS* variable.
```sh
# Compile on docker and create container
make docker_build

# Launch on docker
make docker_run

# Compile and launch
make docker
```




### Local Deploy
#### Configuration
Edit the *config.ini* file and change the host ip (cloudera_manager) in the *target* section by your Cloudera Manager IP addr.  If you want to use "cloudera_manager" as a name-domain, add the entry to your */etc/hosts* file.
```sh
# Compile on local
make local_build

# Launch on local with Makefile
make local_run

# Launch on local on shell
./cloudera_exporter --config-file config.ini

# Compile and launch
make local
```

Cloudera Exporter args:
```sh
  usage: cloudera_exporter [<flags>]

Flags:
  -h, --help                     Show context-sensitive help (also try --help-long and --help-man).
      --config-file="config.ini" Path to ini file.
      --web.listen-address=""    Listent Address.
      --num-procs=0              Number Processes for parallel execution
      --log-level=0              Debug Log Mode
      --timeout-offset=0.25      Time to subtract from timeout in seconds.
      --version                  Show application version.
```




### Clean Environment
To clean the local binary and the cloudera_exporter container
```sh
make clean
```




### Test if is running
Test if cloudera_exporter is running
```sh
make test
```




## Metrics Catalog
To get a list of all the metrics of each Scraper, read the  Scraper, read the [METRICS_CATALOG.md](METRICS_CATALOG.md) file.




### Ownership
**Authors:**
 - Alejandro Villegas - LEAD Developer (<avillegas@keedio.com>)
 - Roberto Manzano - Developer (<rmanzano@keedio.com>)
 - Julian Montoro - Developer (<rmanzano@keedio.com>)
 - Raul Barroso - Idea & Basic model (<rbarroso@keedio.com>)


**Owners:**
 - Keedio Sistemas (<systems@keedio.com>)  
![KEEDIO](https://www.cibernos.com/web-content/uploads/2015/02/keedio.png "KEEDIO") 
