/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adapter that causes metrics from a metric registry to be published to JMX.
 *
 */
public class JmxReporterMetricsRegistryRegistrationListener implements MetricsRegistryRegistrationListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(JmxReporterMetricsRegistryRegistrationListener.class);

  @Override
  public void onMetricsRegistryRegistered(PinotMetricsRegistry metricsRegistry) {
    LOGGER.info("Registering JmxReporterMetricsRegistryRegistrationListener");
    PinotMetricUtils.makePinotJmxReporter(metricsRegistry).start();
    LOGGER.info("Number of metrics in metricsRegistry: {}", metricsRegistry.allMetrics().size());
  }
}
