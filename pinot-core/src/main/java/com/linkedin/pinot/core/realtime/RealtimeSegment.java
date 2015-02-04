package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.RecordReader;


public interface RealtimeSegment extends MutableIndexSegment {

  /**
   * Schema has the list of all dimentions, metrics and time columns.
   * it also should contain data type for each of them
   * for Time column it expects that there is a
   * Time column Type (int, long etc) and an associated TimeUnit (days, hours etc)
   * @param dataSchema
   */
  public void init(Schema dataSchema);

  /**
   * once seal method is called,
   * documents will no longer be accepted for indexing.
   * @return
   */
  public boolean seal();

  /**
   * returns a RecordReader implementation
   * which can be used to create an offline segment.
   *
   * @return
   */
  public RecordReader getRecordReader();

  /**
   * this will return the total number of documents that have been indexed to far,
   * this is so that the indexing Coordination (if it chooses to) can decided
   * when to convert this segment to immutable.
   * @return
   */
  public int getCurrentDocumentsIndexedCount();

}
