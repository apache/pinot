package com.linkedin.pinot.core.indexsegment;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;


/**
 * This is the interface of index segment. The index type of index segment
 * should be one of the supported {@link com.linkedin.pinot.core.indexsegment.IndexType
 * IndexType}.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface IndexSegment {
  /**
   * @return
   */
  public IndexType getIndexType();

  /**
   * @return
   */
  public String getSegmentName();

  /**
   * @return
   */
  public String getAssociatedDirectory();

  /**
   * @return SegmentMetadata
   */
  public SegmentMetadata getSegmentMetadata();

  /**
   *
   * @param columnName
   * @return
   */
  DataSource getDataSource(String columnName);

  /**
   *
   * @param columnName
   * @param p
   * @return
   */
  DataSource getDataSource(String columnName, Predicate p) throws Exception;

  /**
   * @return
   */
  String[] getColumnNames();
}
