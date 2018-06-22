/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;


public class RecordUtil {

  /**
   * Get Integer values for the given data.
   *
   * @param data list of Object
   * @return Integer array.
   */
  public static int[] getIntValues(List<Object> data) {

    int [] intData = new int[data.size()];
    for ( int i = 0; i < data.size(); i++) {
      intData[i] = (int)data.get(i);
    }

    return intData;
  }

  /**
   * Get Long values for the given data.
   *
   * @param data list of Object
   * @return Integer array.
   */
  public static long[] getLongValues(List<Object> data) {

    long [] longData = new long[data.size()];
    for ( int i = 0; i < data.size(); i++) {
      longData[i] = (long)data.get(i);
    }

    return longData;
  }

  /**
   * Get Float values for the given data.
   *
   * @param data list of Object
   * @return float array.
   */
  public static float[] getFloatValues(List<Object> data) {
    float [] floatData = new float[data.size()];
    for ( int i = 0; i < data.size(); i++) {
      floatData[i] = (float)data.get(i);
    }

    return floatData;
  }

  /**
   * Get Double values for the given data.
   *
   * @param data list of Object
   * @return float array.
   */
  double[] getDoubleValues(List<Object> data) {
    double [] floatData = new double[data.size()];
    for ( int i = 0; i < data.size(); i++) {
      floatData[i] = (double)data.get(i);
    }

    return floatData;
  }
}
