package com.linkedin.pinot.core.query.utils;



public class SimpleIndexSegment {

  /*  private final Map<String, ColumnarReader> _dataMap;
  private final SegmentMetadata _segmentMetadata;
  private final String _segmentName;
  private String _associatedDir;
  private final IndexType _indexType;
  private final long _numRecords;

  public SimpleIndexSegment(long numRecords, Map<String, ColumnarReader> dataMap) {
    _indexType = IndexType.simple;
    _dataMap = dataMap;
    _numRecords = numRecords;
    _segmentMetadata = new SimpleSegmentMetadata();
    ((SimpleSegmentMetadata) _segmentMetadata).setSize(_numRecords);
    _segmentName = "simpleIndexSegment-" + System.currentTimeMillis();
  }

  public SimpleIndexSegment(long numRecords, Map<String, ColumnarReader> dataMap, Schema schema) {
    _indexType = IndexType.simple;
    _dataMap = dataMap;
    _numRecords = numRecords;
    _segmentMetadata = new SimpleSegmentMetadata("resourceName", "tableName", schema);
    ((SimpleSegmentMetadata) _segmentMetadata).setSize(_numRecords);
    _segmentName = "simpleIndexSegment-" + System.currentTimeMillis();
  }

  public SimpleIndexSegment(long numRecords, String resourceName, String tableName, Map<String, ColumnarReader> dataMap) {
    _indexType = IndexType.simple;
    _dataMap = dataMap;
    _numRecords = numRecords;
    _segmentMetadata = new SimpleSegmentMetadata(resourceName, tableName);
    ((SimpleSegmentMetadata) _segmentMetadata).setSize(_numRecords);
    _segmentName = "simpleIndexSegment-" + System.currentTimeMillis();
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
  public String getAssociatedDirectory() {
    return _associatedDir;
  }

  private Iterator<Integer> getDocIdIterator() {
    final Iterator<Integer> iterator = new Iterator<Integer>() {
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
  public DataSource getDataSource(String columnName) {
    return new SimpleIndexDataSource(_dataMap.get(columnName), _numRecords);
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    return new SimpleIndexDataSource(_dataMap.get(columnName), _numRecords);
  }

  @Override
  public String[] getColumnNames() {
    return _dataMap.keySet().toArray(new String[0]);
  }*/
}
