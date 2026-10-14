#### What is Prometheus?

Prometheus is an open-source event monitoring system. It records real-time metrics in a time series database and can be queried with its own query language PromQL. Prometheus has 4 metric types (Gauge, Counter, Histogram, Summary).

Prometheus is not a full-fledged dashboarding solution and needs to be hooked up with Grafana to generate dashboards. 

#### How do Pinot metrics end up in Prometheus?

Currently, Pinot metrics are exposed as JMX mbeans through the PinotJmxReporter. 
These JMX mbeans are consumed by Prometheus using the [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter).
Fairly comprehensive Prometheus JMX Exporter config files can be found under
[docker/images/pinot/etc/jmx_prometheus_javaagent/configs/](docker/images/pinot/etc/jmx_prometheus_javaagent/configs/).

See the [Pinot docs](https://docs.pinot.apache.org/operators/operating-pinot/monitoring) for more info.

#### How can I view and test metrics?

First, you need to make sure the metrics are exposed though JMX. 
Note, that if no metric has been published (e.g. no values recorded), the metric will not show up in JMX or Prometheus server.
With a local Pinot deployment, you can launch `jconsole`, select your local deployment and view all the metrics exposed as jmx mbeans.
Alternative, you can use [jmxterm](https://docs.cyclopsgroup.org/jmxterm) in order to read them using a CLI.
To see if the metrics are being consumed with the Prometheus JMX Exporter and your config file, 
you can set the JAVA_OPTS env variable before running Pinot locally.

`export JAVA_OPTS="-javaagent:jmx_prometheus_javaagent.jar=8080:pinot.yml -Xms4G -Xmx4G -XX:MaxDirectMemorySize=30g -Dlog4j2.configurationFile=conf/pinot-admin-log4j2.xml -Dplugins.dir=$BASEDIR/plugins"
bin/pinot-admin.sh ....
`

Remember that it is recommended to use service specific prometheus configurations (like `broker.yml`, `server.yml`, etc)
in production instead of `pinot.xml`.

This will expose a port at 8080 to dump metrics as Prometheus format for Prometheus scraper to fetch.

#### Expose a scrape endpoint with Dropwizard

The same Java agent publishes an HTTP `/metrics` endpoint with `pinot-dropwizard`. For a broker, configure:

```properties
pinot.broker.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
```

Use the matching role YAML. The role-specific rules support both Yammer and native Dropwizard MBeans and
keep the existing Prometheus metric names and labels.

For a broker in the Pinot Docker image, append the agent option before starting Pinot:

```sh
export JAVA_OPTS="$JAVA_OPTS -javaagent:/opt/pinot/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent.jar=9404:/opt/pinot/etc/jmx_prometheus_javaagent/configs/broker.yml"
bin/pinot-admin.sh StartBroker -configFileName /opt/pinot/configs/broker.conf
```

Docker images include the agent JAR and exporter configurations. For a binary installation, provide the
[JMX exporter Java agent](https://prometheus.github.io/jmx_exporter/1.5.0/deployment/java-agent/) and the YAML
files separately, and substitute their paths above. Use `controller.yml`, `server.yml`, or `minion.yml` for
the corresponding service. Choose a different agent port for each process sharing a host.

Publish the chosen port when using Docker, or expose it in the deployment's Kubernetes networking, so that
Prometheus can reach the endpoint. For port 9404:

```sh
curl http://localhost:9404/metrics
```

Configure Prometheus to scrape that address:

```yaml
scrape_configs:
  - job_name: pinot-broker
    static_configs:
      - targets: ["broker-host:9404"]
```

Standard Pinot timer samples remain in milliseconds and meter rates per second. Direct JMX consumers still
see the selected backend's native MBean schema; custom metrics may require additional exporter rules.
