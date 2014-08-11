package com.linkedin.pinot.core.query.utils;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class IndexSegmentUtils {

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs) {
    return new SimpleIndexSegment(numberOfDocs, getDataMap(numberOfDocs));
  }

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs, String resourceName,
      String tableName) {
    return new SimpleIndexSegment(numberOfDocs, resourceName, tableName, getDataMap(numberOfDocs));
  }

  private static Map<String, ColumnarReader> getDataMap(int numberOfDocs) {
    final Map<String, ColumnarReader> dataMap = new HashMap<String, ColumnarReader>();
    dataMap.put("met", getColumnarReader(numberOfDocs));
    return dataMap;
  }

  private static ColumnarReader getColumnarReader(int numRecords) {
    final double[] doubleArray = new double[numRecords];

    for (int i = 0; i < numRecords; ++i) {
      doubleArray[i] = i;
    }
    ColumnarReader columnReader = new ColumnarReader() {
      @Override
      public String getStringValue(int docId) {
        return doubleArray[docId] + "";
      }

      @Override
      public long getLongValue(int docId) {
        return (long) doubleArray[docId];
      }

      @Override
      public int getIntegerValue(int docId) {
        return (int) doubleArray[docId];
      }

      @Override
      public float getFloatValue(int docId) {
        return (float) doubleArray[docId];
      }

      @Override
      public double getDoubleValue(int docId) {
        return doubleArray[docId];
      }

      @Override
      public Object getRawValue(int docId) {
        return doubleArray[docId];
      }
    };
    return columnReader;
  }

}
