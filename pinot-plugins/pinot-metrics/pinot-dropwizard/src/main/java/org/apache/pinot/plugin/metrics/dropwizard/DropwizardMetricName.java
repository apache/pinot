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
package org.apache.pinot.plugin.metrics.dropwizard;

import java.util.Objects;
import org.apache.pinot.spi.metrics.PinotMetricName;


public final class DropwizardMetricName implements PinotMetricName {
  private final String _metricName;

  public DropwizardMetricName(Class<?> klass, String name) {
    this(name);
  }

  public DropwizardMetricName(String metricName) {
    _metricName = metricName;
  }

  @Override
  public String getMetricName() {
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
    if (!(obj instanceof DropwizardMetricName)) {
      return false;
    }
    DropwizardMetricName that = (DropwizardMetricName) obj;
    return Objects.equals(_metricName, that._metricName);
  }

  /**
   * Overrides hashCode method by calling the hashCode method from the actual metric name.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(_metricName);
  }

  @Override
  public String toString() {
    return _metricName;
  }
}
