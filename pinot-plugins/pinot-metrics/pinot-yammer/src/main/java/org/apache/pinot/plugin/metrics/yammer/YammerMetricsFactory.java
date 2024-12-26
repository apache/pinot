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
package org.apache.pinot.plugin.metrics.yammer;

import com.google.auto.service.AutoService;
import java.util.function.Function;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


@AutoService(PinotMetricsFactory.class)
@MetricsFactory
public class YammerMetricsFactory implements PinotMetricsFactory {
  private PinotMetricsRegistry _pinotMetricsRegistry = null;

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    if (_pinotMetricsRegistry == null) {
      _pinotMetricsRegistry = new YammerMetricsRegistry();
    }
    return _pinotMetricsRegistry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    return new YammerMetricName(klass, name);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    return new YammerGauge<T>(condition);
  }

  @Override
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    return new YammerJmxReporter(metricsRegistry);
  }

  @Override
  public String getMetricsFactoryName() {
    return "Yammer";
  }
}
