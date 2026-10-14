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

Yammer remains bundled and is still the default when no metrics factory is configured. A default change and
removal release will be decided separately; see [#19871](https://github.com/apache/pinot/issues/19871).

## Migrate to Dropwizard

1. Ensure `pinot-dropwizard` and its runtime dependencies are available to each affected service. Both metrics
   plugins ship in the standard Pinot binary distribution; custom builds, classpaths, and plugin filters may
   need to be updated.
2. Set the applicable property in each service's configuration file:

   ```properties
   pinot.broker.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.controller.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.server.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   pinot.minion.metrics.factory.className=org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory
   ```

3. Restart each affected service to load the new factory. Validate the migration in a test environment before
   rolling it out to production.

The backends can differ in JMX object names and attributes, exported Prometheus metric names and labels, and
timer/histogram units and distributions. Compare the exported metrics and update scrape rules, dashboards, and
alerts as needed. Review custom Yammer reporters/listeners and explicit Yammer factory references in compound
metrics configurations as part of the migration.

To roll back while Yammer remains available, restore the previous factory setting (or remove the new setting
if you previously relied on the Yammer default), then restart the affected services.
