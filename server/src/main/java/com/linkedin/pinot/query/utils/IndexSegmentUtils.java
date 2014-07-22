package com.linkedin.pinot.query.utils;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.index.segment.ColumnarReader;
import com.linkedin.pinot.index.segment.IndexSegment;


public class IndexSegmentUtils {

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs) {
    final int numRecords = numberOfDocs;

    final Map<String, ColumnarReader> dataMap = new HashMap<String, ColumnarReader>();
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

    dataMap.put("met", columnReader);

    IndexSegment indexSegment = new SimpleIndexSegment(numRecords, dataMap);
    return indexSegment;
  }
}
