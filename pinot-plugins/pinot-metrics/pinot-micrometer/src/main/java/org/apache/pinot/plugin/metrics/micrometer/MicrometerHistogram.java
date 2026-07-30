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
package org.apache.pinot.plugin.metrics.micrometer;

import io.micrometer.core.instrument.DistributionSummary;
import org.apache.pinot.spi.metrics.PinotHistogram;


/// Native Micrometer implementation of {@link PinotHistogram}, backed by a Micrometer {@link DistributionSummary}.
///
/// <p>A Micrometer distribution summary exports {@code _count} and {@code _sum} Prometheus series (plus any
/// configured client-side percentiles or buckets).
public class MicrometerHistogram implements PinotHistogram {
  private final DistributionSummary _histogram;

  public MicrometerHistogram(DistributionSummary histogram) {
    _histogram = histogram;
  }

  @Override
  public Object getHistogram() {
    return _histogram;
  }

  @Override
  public Object getMetric() {
    return _histogram;
  }
}
