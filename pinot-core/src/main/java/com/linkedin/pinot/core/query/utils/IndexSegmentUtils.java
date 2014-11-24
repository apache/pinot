package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;



public class IndexSegmentUtils {

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs) {
    return null;
  }

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs, Schema schema) {
    return null;
  }

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs, String resourceName,
      String tableName) {
    return null;
  }


  /*
  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs) {
    return new SimpleIndexSegment(numberOfDocs, getDataMap(numberOfDocs));
  }

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs, Schema schema) {
    return new SimpleIndexSegment(numberOfDocs, getDataMap(numberOfDocs), schema);
  }

  public static IndexSegment getIndexSegmentWithAscendingOrderValues(int numberOfDocs, String resourceName,
      String tableName) {
    return new SimpleIndexSegment(numberOfDocs, resourceName, tableName, getDataMap(numberOfDocs));
  }

  private static Map<String, ColumnarReader> getDataMap(int numberOfDocs) {
    final Map<String, ColumnarReader> dataMap = new HashMap<String, ColumnarReader>();
    dataMap.put("met", getColumnarReader(numberOfDocs));
    dataMap.put("dim0", getDim0ColumnarReader(numberOfDocs));
    dataMap.put("dim1", getDim1ColumnarReader(numberOfDocs));
    return dataMap;
  }

  private static ColumnarReader getDim0ColumnarReader(int numberOfDocs) {
    final double[] doubleArray = new double[numberOfDocs];

    for (int i = 0; i < numberOfDocs; ++i) {
      doubleArray[i] = i % 10;
    }
    final ColumnarReader columnReader = new ColumnarReader() {
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

      @Override
      public DataType getDataType() {
        return DataType.DOUBLE;
      }

      @Override
      public int getDictionaryId(int docId) {
        return (int) doubleArray[docId];
      }

      @Override
      public String getStringValueFromDictId(int dictId) {
        return dictId + "";
      }

    };
    return columnReader;
  }

  private static ColumnarReader getDim1ColumnarReader(int numberOfDocs) {
    final double[] doubleArray = new double[numberOfDocs];

    for (int i = 0; i < numberOfDocs; ++i) {
      doubleArray[i] = i % 100;
    }
    final ColumnarReader columnReader = new ColumnarReader() {
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

      @Override
      public DataType getDataType() {
        return DataType.DOUBLE;
      }

      @Override
      public int getDictionaryId(int docId) {
        return (int) doubleArray[docId];
      }

      @Override
      public String getStringValueFromDictId(int dictId) {
        return dictId + "";
      }

    };
    return columnReader;
  }

  private static ColumnarReader getColumnarReader(int numRecords) {
    final double[] doubleArray = new double[numRecords];

    for (int i = 0; i < numRecords; ++i) {
      doubleArray[i] = i;
    }
    final ColumnarReader columnReader = new ColumnarReader() {

      public double[] getDoubleArray() {
        return doubleArray;
      }

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

      @Override
      public DataType getDataType() {
        return DataType.DOUBLE;
      }

      @Override
      public int getDictionaryId(int docId) {
        return (int) doubleArray[docId];
      }

      @Override
      public String getStringValueFromDictId(int dictId) {
        return dictId + "";
      }
    };
    return columnReader;
  }*/

}
