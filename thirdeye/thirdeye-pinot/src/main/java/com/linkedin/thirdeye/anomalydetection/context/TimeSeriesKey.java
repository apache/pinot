/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;

public class TimeSeriesKey {
  private String metricName = "";
  private DimensionMap dimensionMap = new DimensionMap();

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public DimensionMap getDimensionMap() {
    return dimensionMap;
  }

  public void setDimensionMap(DimensionMap dimensionMap) {
    this.dimensionMap = dimensionMap;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimeSeriesKey) {
      TimeSeriesKey other = (TimeSeriesKey) o;
      return ObjectUtils.equals(metricName, other.metricName)
          && ObjectUtils.equals(dimensionMap, other.dimensionMap);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, dimensionMap);
  }
}
