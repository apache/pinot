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

import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.util.Map;

/**
 * Component inject to IndexSegment for handling extra logic for upsert-enabled pinot realtime table
 *
 * This feature currently only support Low-level consumer.
 */
@InterfaceStability.Evolving
public interface IndexSegmentCallback {

  /**
   * Initialize the callback from IndexSegment. This happens in constructor of IndexSegment implementation class
   *
   * In upsert-enabled tables callback, this method will initialize the necessary virtual columns for upsert table
   * (offset mapping, $validFrom, $validUntil columns)
   *
   * this method ensure that virtual columns index (forward/reverted index) can be created properly and load
   * any existing data for the virtual column from local storage
   *
   * @param segmentMetadata the metadata associated with the current segment
   * @param columnIndexContainerMap mapping of necessary column name and the associate columnIndexContainer
   */
  void onIndexSegmentCreation(SegmentMetadata segmentMetadata, Map<String, ColumnIndexContainer> columnIndexContainerMap);

  /**
   * Perform operation from the callback for the given row after it has been processed and index.
   * This method happens after indexing finished in MutableSegmentImpl.index(GenericRow, RowMetadata) method
   * It will only trigger in LLC and if the row is indexed but not aggregated.
   *
   * In upsert-enabled tables callback, this method will build offset mapping and update virtual columns for the column if necessary
   *
   * This method is similar to
   * {@link org.apache.pinot.core.data.manager.callback.DataManagerCallback#onRowIndexed(GenericRow, StreamPartitionMsgOffset)}
   * However, the other method don't have information to docID and it is necessary for upsert virtual columns
   * to have these information to build the forward index and offset columns to build mapping between offset -> docId
   * we also might need to update virtual column forward-index for the newly ingested row
   *
   * @param row the current pinot row we just indexed into the current IndexSegment
   * @param docId the docId of this record
   */
  void onRowIndexed(GenericRow row, int docId);

}
