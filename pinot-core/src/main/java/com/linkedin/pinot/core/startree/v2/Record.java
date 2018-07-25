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
package com.linkedin.pinot.core.startree.v2;

import java.util.List;


/**
 * Utility class to mimic behavior of 'generic row'.
 */
public class Record {
  protected int[] _dimensionValues;
  protected List<Object> _metricValues;

  /**
   * Return the metric values for the record.
   *
   * @return 'String' value
   */
  public List<Object> getMetricValues() {
    return _metricValues;
  }

  /**
   * Set the metric values for the record.
   *
   */
  public void setMetricValues(List<Object> metricValues) {
    _metricValues = metricValues;
  }

  /**
   * Return the dimension values for the record.
   *
   * @return 'String' value
   */
  public int[] getDimensionValues() {
    return _dimensionValues;
  }

  /**
   * Set the dimension values for the record.
   *
   */
  public void setDimensionValues(int[] dimensionValues) {
    _dimensionValues = dimensionValues;
  }
}
