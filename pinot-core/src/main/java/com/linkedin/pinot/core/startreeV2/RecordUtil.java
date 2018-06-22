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
