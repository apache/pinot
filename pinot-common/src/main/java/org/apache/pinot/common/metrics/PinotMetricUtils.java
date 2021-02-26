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
package org.apache.pinot.common.metrics;

import java.util.function.Function;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.common.metrics.yammer.YammerGauge;
import org.apache.pinot.common.metrics.yammer.YammerJmxReporter;
import org.apache.pinot.common.metrics.yammer.YammerMetricName;
import org.apache.pinot.common.metrics.yammer.YammerMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotMetricUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotMetricUtils.class);
  public static final String LIBRARY_NAME_KEY = "libraryName";
  public static final String YAMMER_KEY = "yammer";

  private static String LIBRARY_TO_USE = YAMMER_KEY;

  public static void init(PinotConfiguration metricsConfiguration)
      throws InvalidConfigException {
    String libraryName = metricsConfiguration.getProperty(PinotMetricUtils.LIBRARY_NAME_KEY);
    if (libraryName == null) {
      return;
    }
    switch (libraryName) {
      case YAMMER_KEY:
        LIBRARY_TO_USE = YAMMER_KEY;
        break;
      // TODO: support more libraries.
      default:
        throw new InvalidConfigException("PinotMetricsRegistry for " + libraryName + " cannot be initialized.");
    }
    LOGGER.info("Setting metric library to: " + LIBRARY_TO_USE);
  }

  public static PinotMetricsRegistry getPinotMetricsRegistry() {
    switch (LIBRARY_TO_USE) {
      case YAMMER_KEY:
        return new YammerMetricsRegistry();
      //TODO: support more libraries.
      default:
        return new YammerMetricsRegistry();
    }
  }

  public static PinotMetricName generatePinotMetricName(Class<?> klass, String name) {
    switch (LIBRARY_TO_USE) {
      case YAMMER_KEY:
        return new YammerMetricName(klass, name);
      //TODO: support more libraries.
      default:
        return new YammerMetricName(klass, name);
    }
  }

  public static <T> PinotGauge<T> generatePinotGauge(Function<Void, T> condition) {
    switch (LIBRARY_TO_USE) {
      case YAMMER_KEY:
        return new YammerGauge<T>(condition);
      //TODO: support more libraries.
      default:
        return new YammerGauge<T>(condition);
    }
  }

  public static PinotJmxReporter generatePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    switch (LIBRARY_TO_USE) {
      case YAMMER_KEY:
        return new YammerJmxReporter(metricsRegistry);
      //TODO: support more libraries.
      default:
        return new YammerJmxReporter(metricsRegistry);
    }
  }
}
