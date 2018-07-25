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
import java.util.ArrayList;


/**
 * Utility class for Record class.
 */
public class RecordUtil {

  /**
   * Get Integer values for the given data.
   *
   * @param data list of Object
   *
   * @return Integer array.
   */
  public static List<Integer> getIntValues(List<Object> data) {

    List<Integer> intData = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      intData.add((int) data.get(i));
    }

    return intData;
  }

  /**
   * Get Long values for the given data.
   *
   * @param data list of Object
   *
   * @return Integer array.
   */
  public static List<Long> getLongValues(List<Object> data) {

    List<Long> longData = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      longData.add((long) data.get(i));
    }

    return longData;
  }

  /**
   * Get Float values for the given data.
   *
   * @param data list of Object
   *
   * @return float array.
   */
  public static List<Float> getFloatValues(List<Object> data) {
    List<Float> floatData = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      floatData.add((float) data.get(i));
    }

    return floatData;
  }

  /**
   * Get Double values for the given data.
   *
   * @param data list of Object
   *
   * @return double array.
   */
  public static List<Double> getDoubleValues(List<Object> data) {
    List<Double> doubleData = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      doubleData.add(Double.valueOf(data.get(i).toString()));
    }

    return doubleData;
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
