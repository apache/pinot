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
package org.apache.pinot.common.metrics.yammer;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import org.apache.pinot.common.metrics.base.PinotCounter;
import org.apache.pinot.common.metrics.base.PinotGauge;
import org.apache.pinot.common.metrics.base.PinotHistogram;
import org.apache.pinot.common.metrics.base.PinotMetered;
import org.apache.pinot.common.metrics.base.PinotMetricName;
import org.apache.pinot.common.metrics.base.PinotMetricProcessor;
import org.apache.pinot.common.metrics.base.PinotTimer;


public class YammerMetricProcessor<T> implements PinotMetricProcessor<T> {
  private MetricProcessor<T> _metricProcessor;

  public YammerMetricProcessor(MetricProcessor<T> metricProcessor) {
    _metricProcessor = metricProcessor;
  }

  @Override
  public void processMeter(PinotMetricName name, PinotMetered meter, T context)
      throws Exception {
    _metricProcessor.processMeter((MetricName) name.getMetricName(), (Metered) meter.getMetered(), context);
  }

  @Override
  public void processCounter(PinotMetricName name, PinotCounter counter, T context)
      throws Exception {
    _metricProcessor.processCounter((MetricName) name.getMetricName(), (Counter) counter.getCounter(), context);
  }

  @Override
  public void processHistogram(PinotMetricName name, PinotHistogram histogram, T context)
      throws Exception {
    _metricProcessor.processHistogram((MetricName) name.getMetricName(), (Histogram) histogram.getHistogram(), context);
  }

  @Override
  public void processTimer(PinotMetricName name, PinotTimer timer, T context)
      throws Exception {
    _metricProcessor.processTimer((MetricName) name.getMetricName(), (Timer) timer.getTimer(), context);
  }

  @Override
  public void processGauge(PinotMetricName name, PinotGauge<?> gauge, T context)
      throws Exception {
    _metricProcessor.processGauge((MetricName) name.getMetricName(), (Gauge<?>) gauge.getGauge(), context);
  }
}
