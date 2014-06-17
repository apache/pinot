package com.linkedin.pinot.query.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.ColumnarReader;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;


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
    };

    dataMap.put("met", columnReader);

    IndexSegment indexSegment = new IndexSegment() {

      Map<String, ColumnarReader> dataMap1 = dataMap;

      @Override
      public void setSegmentName(String segmentName) {
        // TODO Auto-generated method stub

      }

      @Override
      public void setSegmentMetadata(SegmentMetadata segmentMetadata) {
        // TODO Auto-generated method stub

      }

      @Override
      public void setAssociatedDirectory(String associatedDirectory) {
        // TODO Auto-generated method stub

      }

      @Override
      public Iterator<RowEvent> processFilterQuery(FilterQuery query) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getSegmentName() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public SegmentMetadata getSegmentMetadata() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public IndexType getIndexType() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Iterator<Integer> getDocIdIterator(FilterQuery query) {

        return getDocIdIterator();
      }

      @Override
      public String getAssociatedDirectory() {
        // TODO Auto-generated method stub
        return null;
      }

      private Iterator<Integer> getDocIdIterator() {
        Iterator<Integer> iterator = new Iterator<Integer>() {
          int i = 0;

          @Override
          public boolean hasNext() {
            // TODO Auto-generated method stub
            return (i < numRecords);
          }

          @Override
          public Integer next() {
            return i++;
          }

          @Override
          public void remove() {
            // TODO Auto-generated method stub

          }
        };
        return iterator;
      }

      @Override
      public ColumnarReader getColumnarReader(String column) {
        return dataMap1.get(column);
      }
    };
    return indexSegment;
  }

}
