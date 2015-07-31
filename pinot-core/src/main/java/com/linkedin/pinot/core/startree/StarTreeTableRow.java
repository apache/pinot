/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import java.util.List;
import java.util.Objects;

public class StarTreeTableRow {
  private final List<Integer> dimensions;
  private final List<Number> metrics;

  StarTreeTableRow(List<Integer> dimensions, List<Number> metrics) {
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  public List<Integer> getDimensions() {
    return dimensions;
  }

  public List<Number> getMetrics() {
    return metrics;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dimensions);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeTableRow)) {
      return false;
    }
    StarTreeTableRow e = (StarTreeTableRow) o;
    return Objects.equals(e.getDimensions(), dimensions); // n.b. NOT metrics too
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("dimensions", dimensions)
        .add("metrics", metrics)
        .toString();
  }
}
