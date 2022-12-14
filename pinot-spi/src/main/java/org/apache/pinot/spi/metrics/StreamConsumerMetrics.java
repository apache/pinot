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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;


public class StreamConsumerMetrics extends AbstractMetrics<NoOpQueryPhase,
    StreamConsumerMeter, StreamConsumerGauge, StreamConsumerTimer> {
  private static final AtomicReference<StreamConsumerMetrics> STREAM_CONSUMER_METRICS_INSTANCE
      = new AtomicReference<>();

  public StreamConsumerMetrics(String metricPrefix, PinotMetricsRegistry metricsRegistry,
      Class clazz) {
    super(metricPrefix, metricsRegistry, clazz, false, Collections.emptySet());
  }

  /**
   * register the StreamConsumerMetrics onto this class, so that we don't need to pass it down as a parameter
   */
  public static boolean register(StreamConsumerMetrics streamConsumerMetrics) {
    return STREAM_CONSUMER_METRICS_INSTANCE.compareAndSet(null, streamConsumerMetrics);
  }

  @Override
  protected NoOpQueryPhase[] getQueryPhases() {
    return new NoOpQueryPhase[0];
  }

  @Override
  protected StreamConsumerMeter[] getMeters() {
    return new StreamConsumerMeter[0];
  }

  @Override
  protected StreamConsumerGauge[] getGauges() {
    return new StreamConsumerGauge[0];
  }
}
