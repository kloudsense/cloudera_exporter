# Contributing

Keedio uses GitHub to manage reviews of pull requests.

This repository is private.


* If you have a trivial fix or improvement, go ahead and create a pull request, addressing (with `@...`) the maintainer of this repository (see [MAINTAINERS.md](MAINTAINERS.md)) in the description of the pull request.

* If you plan to do something more involved, first discuss your ideas on our [mailing list](https://groups.google.com/forum/?fromgroups#!forum/prometheus-developers).  This will avoid unnecessary work and surely give you and us a good deal of inspiration.

* Relevant coding style guidelines are the [Go Code Review Comments](https://code.google.com/p/go-wiki/wiki/CodeReviewComments) and the _Formatting and style_ section of Peter Bourgon's [Go: Best Practices for Production Environments](http://peter.bourgon.org/go-in-production/#formatting-and-style).


## Local setup

The easiest way to make a local development setup is creating a Cloudera VM and starting this exporter with Makefile.

```sh
vim config.ini 
# Check the host and port parameters on [target] section to your Cloudera VM network configuration.
# This exporter can resolve Domain Names if you set up on host parameter, but you need a DNS with the Domain Name of the Cloudera VM to resolve the requests.
# Version parameter is not mandatory, this exporter try to get this value for itself.

# Set up the username and password params on [user] section to your Cloudera VM credentials.

# On [modules] section you can set true/false for enable or disable a scrape module.

# On [system] section you can change the IP and port for deploy this exporter, the number of process to paralelice the scrape process and the level of log.

make local

# Go to this URL http://<YOUR_DEPLOY_IP>:<PORT>/metrics
# Example
curl -s http://192.168.1.100:9200/metrics

```
