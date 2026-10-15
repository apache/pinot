<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pinot Yammer metrics (deprecated)

`pinot-yammer` is deprecated since Pinot 1.6.0. Use the existing `pinot-dropwizard` metrics backend instead.

Dropwizard is the default when no metrics factory is configured. Explicit Yammer factory settings still select
Yammer, which remains bundled. A removal release will be decided separately; see
[#19871](https://github.com/apache/pinot/issues/19871).

## Migrate to Dropwizard

1. Ensure `pinot-dropwizard` and its runtime dependencies are available to each affected service. Both metrics
   plugins ship in the standard Pinot binary distribution; custom builds, classpaths, and plugin filters may
   need to be updated.
2. Remove explicit Yammer factory settings to use the Dropwizard default, or replace them with the applicable
   property in each service's configuration file:

   ```properties
   pinot.broker.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.controller.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.server.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.minion.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   ```

3. To publish a Prometheus scrape endpoint, enable the existing JMX exporter Java agent with
   `configs-dropwizard/<role>.yml`, where `<role>` is `broker`, `controller`, `server`, or `minion`.
   If the agent is already enabled, change its YAML path from `configs/<role>.yml` when switching from Yammer.
   See the
   [agent startup and scrape configuration](../../../docker/images/pinot/etc/jmx_prometheus_javaagent/README.md#expose-a-scrape-endpoint-with-dropwizard).
4. Restart each affected service to load the new factory and exporter configuration. Validate the migration in
   a test environment before rolling it out to production.

The backend-specific role configurations retain the Prometheus metric names and labels for standard Pinot metrics.
Those timers are reported in milliseconds and meter rates per second with either backend.
Native JMX names and attributes can differ, as can sampled timer distributions. Validate custom JMX consumers,
reporters/listeners, and explicit Yammer factory references in compound metrics configurations when migrating.

To roll back while Yammer remains available, explicitly set each affected service's
`pinot.<role>.metrics.factory.className` to `org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory`.
If using the Java agent, also switch its YAML path back to `configs/<role>.yml`. Restart the affected services
after updating the settings. Removing the factory setting selects the Dropwizard default.
