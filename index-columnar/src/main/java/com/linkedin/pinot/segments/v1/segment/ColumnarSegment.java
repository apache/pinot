package com.linkedin.pinot.segments.v1.segment;

import java.util.Iterator;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.operator.DataSource;
import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.ColumnarReader;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;

public class ColumnarSegment implements IndexSegment {

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

@Override
public DataSource getDataSource(String columnName) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public DataSource getDataSource(String columnName, Predicate p) {
	// TODO Auto-generated method stub
	return null;
}

}
