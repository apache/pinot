/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.callback;

import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.io.IOException;

/**
 * component inject to {@link org.apache.pinot.core.data.manager.SegmentDataManager} for handling extra logic for
 * other workflows other than regular append-mode ingestion.
 */
public interface DataManagerCallback {

  void init() throws IOException;

  /**
   * component inject to {@link org.apache.pinot.core.indexsegment.IndexSegment} for handling extra logic for
   * other workflows other than regular append-mode ingestion.
   */
  IndexSegmentCallback getIndexSegmentCallback();

  /**
   * process the row after transformation in the ingestion process
   * this mostly exists such that we can attach offset info to generic rows object.
   * happens after the row has been ingested into pinot but before the row is indexed
   * @param row the row of newly ingested and transformed data from upstream
   * @param offset the offset of this particular pinot record
   */
  void processTransformedRow(GenericRow row, StreamPartitionMsgOffset offset);

  /**
   * process the row after we have finished the index the current row
   * ensure that we can emit the metadata for a primary key entry to the coordinator service
   * @param row the row we just index to the current segment
   * @param offset the offset associated with the current row
   */
  void postIndexProcessing(GenericRow row, StreamPartitionMsgOffset offset);

  /**
   * callback for when a realtime segment data manager done with the current consumption loop
   * for the data associated with a segment
   */
  void postConsumeLoop();

  /**
   * callback when the associated data manager is destroyed by pinot server in call {@link SegmentDataManager#destroy()}
   * ensure we can remove any data initialized by this callback object
   */
  void destroy();
}
