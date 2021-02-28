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

import com.yammer.metrics.core.MetricName;
import org.apache.pinot.spi.metrics.PinotMetricName;


public class YammerMetricName implements PinotMetricName {
  private final MetricName _metricName;

  public YammerMetricName(Class<?> klass, String name) {
    _metricName = new MetricName(klass, name);
  }

  public YammerMetricName(MetricName metricName) {
    _metricName = metricName;
  }

  @Override
  public MetricName getMetricName() {
    return _metricName;
  }

  /**
   * Overrides equals method by calling the equals from the actual metric name.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    YammerMetricName that = (YammerMetricName) obj;
    return _metricName.equals(that._metricName);
  }

  /**
   * Overrides hashCode method by calling the hashCode method from the actual metric name.
   */
  @Override
  public int hashCode() {
    return _metricName.hashCode();
  }
}
