package com.linkedin.pinot.query.utils;

import java.util.Iterator;
import java.util.Map;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.ColumnarReader;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;


public class SimpleIndexSegment implements IndexSegment {

  private Map<String, ColumnarReader> _dataMap;
  private SegmentMetadata _segmentMetadata = new SimpleSegmentMetadata();
  private String _segmentName;
  private String _associatedDir;
  private IndexType _indexType;
  private long _numRecords;

  public SimpleIndexSegment(long numRecords, Map<String, ColumnarReader> dataMap) {
    _indexType = IndexType.simple;
    _dataMap = dataMap;
    _numRecords = numRecords;
  }

  @Override
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public void setSegmentMetadata(SegmentMetadata segmentMetadata) {
    _segmentMetadata = segmentMetadata;
  }

  @Override
  public void setAssociatedDirectory(String associatedDirectory) {
    _associatedDir = associatedDirectory;
  }

  @Override
  public Iterator<RowEvent> processFilterQuery(FilterQuery query) {
    return null;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public IndexType getIndexType() {
    return _indexType;
  }

  @Override
  public Iterator<Integer> getDocIdIterator(FilterQuery query) {

    return getDocIdIterator();
  }

  @Override
  public String getAssociatedDirectory() {
    return _associatedDir;
  }

  private Iterator<Integer> getDocIdIterator() {
    Iterator<Integer> iterator = new Iterator<Integer>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return (i < _numRecords);
      }

      @Override
      public Integer next() {
        return i++;
      }

      @Override
      public void remove() {

      }
    };
    return iterator;
  }

  @Override
  public ColumnarReader getColumnarReader(String column) {
    return _dataMap.get(column);
  }
}
