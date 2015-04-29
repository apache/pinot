package com.linkedin.pinot.core.segment.index;

import java.util.Map;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;


public class IndexSegmentImplNew implements IndexSegment {

  public IndexSegmentImplNew(SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap) {

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
  public String getAssociatedDirectory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
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

  @Override
  public String[] getColumnNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void destroy() {
    // TODO Auto-generated method stub

  }

  @Override
  public int getTotalDocs() {
    // TODO Auto-generated method stub
    return 0;
  }

}
