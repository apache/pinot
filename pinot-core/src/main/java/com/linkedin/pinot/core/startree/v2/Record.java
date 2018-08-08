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
  private int[] _dimensionValues;
  private List<Object> _aggregatedValues;

  /**
   * Return the metric values for the record.
   *
   * @return 'String' value
   */
  public List<Object> getAggregatedValues() {
    return _aggregatedValues;
  }

  /**
   * Set the metric values for the record.
   *
   */
  public void setAggregatedValues(List<Object> aggregatedValues) {
    _aggregatedValues = aggregatedValues;
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

  /**
   * Check if two dimensions data is same or not.
   *
   * @param aD dimension a data.
   * @param bD dimension b data.
   *
   * @return boolean if two dimensions are same or not.
   */
  public static boolean compareDimensions(int[] aD, int[] bD) {
    if (aD.length != bD.length) {
      return false;
    }
    for (int i = 0; i < aD.length; i++) {
      if (aD[i] != bD[i]) {
        return false;
      }
    }
    return true;
  }
}
