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
package org.apache.pinot.plugin.metrics.opentelemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import org.apache.pinot.spi.metrics.PinotHistogram;

/**
 * OpenTelemetryHistogram is the implementation of {@link PinotHistogram} for OpenTelemetry.
 * Actually Pinot-Core does NOT use {@link PinotHistogram} anywhere, so this is just a dummy implementation
 */
public class OpenTelemetryHistogram implements PinotHistogram {
  private final DoubleHistogram _histogram;
  private final Attributes _attributes;

  public OpenTelemetryHistogram(DoubleHistogram histogram, Attributes attributes) {
    _histogram = histogram;
    _attributes = attributes;
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
