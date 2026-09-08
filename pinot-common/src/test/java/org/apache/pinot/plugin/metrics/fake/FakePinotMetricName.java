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
package org.apache.pinot.plugin.metrics.fake;

import java.util.Objects;
import org.apache.pinot.spi.metrics.PinotMetricName;


public class FakePinotMetricName implements PinotMetricName {
  /// Class-qualified, so that two `AbstractMetrics` sharing a metric prefix stay distinct keys -- mirroring the
  /// yammer registry, where the owning class is part of the metric identity.
  private final String _qualifiedName;
  private final String _name;

  public FakePinotMetricName(Class<?> clazz, String name) {
    _qualifiedName = clazz.getName() + "." + name;
    _name = name;
  }

  @Override
  public Object getMetricName() {
    return _qualifiedName;
  }

  /// The bare composed name. Must not include the class qualifier: callers use this to match a registered series
  /// against a metric prefix, which the qualifier would push out of the way.
  @Override
  public String getName() {
    return _name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FakePinotMetricName)) {
      return false;
    }
    return Objects.equals(_qualifiedName, ((FakePinotMetricName) o)._qualifiedName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_qualifiedName);
  }

  @Override
  public String toString() {
    return _qualifiedName;
  }
}
