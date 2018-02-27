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

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface RealtimeSegment extends IndexSegment {

  /**
   * Indexes a record into the segment.
   *
   * @param row Record represented as a {@link GenericRow}
   * @return Whether the segment is full (i.e. cannot index more record into it)
   */
  boolean index(GenericRow row);

  /**
   * Returns the number of records already indexed into the segment.
   *
   * @return The number of records indexed
   */
  int getNumDocsIndexed();

  /**
   * Returns the record for the given document Id.
   *
   * @param docId Document
   * @param reuse Reusable buffer for the record
   * @return The record for the given document Id
   */
  GenericRow getRecord(int docId, GenericRow reuse);
}
