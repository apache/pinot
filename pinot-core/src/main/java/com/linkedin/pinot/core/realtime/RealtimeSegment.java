package com.linkedin.pinot.core.realtime;

import java.util.List;

import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
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
   * returns a RecordReader implementation
   * which can be used to create an offline segment.
   *
   * @return
   */
  public RecordReader getRecordReader();

  /**
   *
   * @param columnName
   * @param docIdCounter
   * @return
   */
  public GenericRow getRawValueRowAt(int docId);

  /**
   * this will return the total number of documents that have been indexed to far,
   * this is so that the indexing Coordination (if it chooses to) can decided
   * when to convert this segment to immutable.
   * @return
   */
  public int getAggregateDocumentCount();

  /**
   * returns the time interval of that datathat has currently been indexed
   * @return
   */
  public Interval getTimeInterval();

}
