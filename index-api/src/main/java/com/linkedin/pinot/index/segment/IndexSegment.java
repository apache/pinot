package com.linkedin.pinot.index.segment;

import java.util.Iterator;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.query.FilterQuery;


/**
 * This is the interface of index segment.
 * The index type of index segment should be one of the supported {@link com.linkedin.pinot.index.IndexType IndexType}.
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
   * @param segmentName
   */
  public void setSegmentName(String segmentName);

  /**
   * @return
   */
  public String getAssociatedDirectory();

  /**
   * @param associatedDirectory
   */
  public void setAssociatedDirectory(String associatedDirectory);

  /**
   * @param segmentMetadata
   */
  public void setSegmentMetadata(SegmentMetadata segmentMetadata);

  /**
   * @return
   */
  public SegmentMetadata getSegmentMetadata();

  /**
   * @param query
   * @return Iterator<Row>
   */
  public Iterator<RowEvent> processFilterQuery(FilterQuery query);
}
