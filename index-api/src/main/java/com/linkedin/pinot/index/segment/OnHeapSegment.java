package com.linkedin.pinot.index.segment;

import java.util.Iterator;
import java.util.Random;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.operator.DataSource;
import com.linkedin.pinot.index.query.FilterQuery;

public class OnHeapSegment implements IndexSegment {
  private static int[] intArray;
  private static final int BLOCK_SIZE = 100000;
  private static final int CARDINALITY = 10000;
  private static final int NUM_DOCS = 10000000;

  static {
    intArray = new int[NUM_DOCS];
    Random r = new Random();
    for (int i = 0; i < intArray.length; i++) {
      intArray[i] = i%10;
    }
  }

  @Override
  public DataSource getDataSource(String columnName) {
    return null;
  }

  @Override
  public DataSource getDataSource(String columnName , Predicate p) {
    return null;
  }

  @Override
  public IndexType getIndexType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSegmentName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSegmentName(String segmentName) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getAssociatedDirectory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setAssociatedDirectory(String associatedDirectory) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setSegmentMetadata(SegmentMetadata segmentMetadata) {
    // TODO Auto-generated method stub

  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<RowEvent> processFilterQuery(FilterQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<Integer> getDocIdIterator(FilterQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ColumnarReader getColumnarReader(String column) {
    // TODO Auto-generated method stub
    return null;
  }
}
