/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * @param docId
   * @return
   */
  public GenericRow getRawValueRowAt(int docId, GenericRow row);

  /**
   * this will return the total number of documents that have been indexed to far,
   * this is so that the indexing Coordination (if it chooses to) can decided
   * when to convert this segment to immutable.
   * @return
   */
  @Override
  public int getAggregateDocumentCount();

  /**
   * returns the time interval of that datathat has currently been indexed
   * @return
   */
  public Interval getTimeInterval();

}
