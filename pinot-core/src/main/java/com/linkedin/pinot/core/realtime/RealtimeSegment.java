package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface RealtimeSegment extends IndexSegment {

  /**
   * Schema has the list of all dimentions, metrics and time columns.
   * it also should contain data type for each of them
   * for Time column it expects that there is a
   * Time column Type (int, long etc) and an associated TimeUnit (days, hours etc)
   * @param dataSchema
   */
  public void init(Schema dataSchema);

  /**
   * expects a generic row that has all the columns
   * specified in the schema which was used to
   * initialize the realtime segment
   * @param row
   */
  public void index(GenericRow row);

  /**
   * once seal method is called,
   * documents will no longer be accepted for indexing.
   * @return
   */
  public boolean seal();

  /**
   * returns a temporary location where the
   * converted offline segment has been persisted.
   *
   * @return
   */
  public String toImmutable();

  /**
   * this will return the total number of documents that have been indexed to far,
   * this is so that the indexing Coordination (if it chooses to) can decided
   * when to convert this segment to immutable.
   * @return
   */
  public int getCurrentDocumentsIndexedCount();

}
