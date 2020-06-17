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

import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.util.Map;

/**
 * Component inject to {@link IndexSegment} for handling extra logic for
 * upsert-enabled pinot ingestion mode
 */
@InterfaceStability.Evolving
public interface IndexSegmentCallback {

  /**
   * Initialize the callback from {@link IndexSegment}. This happens in constructor of IndexSegment implementation class
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will initialize the necessary virtual columns for upsert table
   * (offset mapping, $validFrom, $validUntil columns)
   *
   * this method ensure that virtual columns index (forward/reverted index) can be created properly and load
   * any existing data for the virtual column from local storage
   *
   * @param segmentMetadata the metadata associated with the current segment
   * @param columnIndexContainerMap mapping of necessary column name and the associate columnIndexContainer
   */
  void init(SegmentMetadata segmentMetadata, Map<String, ColumnIndexContainer> columnIndexContainerMap);

  /**
   * Perform operation from the callback for the given row after it has been processed and index.
   * This method happens after indexing finished in MutableSegmentImpl.index(GenericRow, RowMetadata) method
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will build offset mapping and update virtual columns for the column if necessary
   *
   * This method is similar to
   * {@link org.apache.pinot.core.data.manager.callback.DataManagerCallback#postIndexProcessing(GenericRow, StreamPartitionMsgOffset)}
   * However, the other method don't have information to docID and it is necessary for upsert virtual columns
   * to have these information to build the forward index and offset columns to build mapping between offset -> docId
   * we also might need to update virtual column forward-index for the newly ingested row
   *
   * @param row the current pinot row we just indexed into the current IndexSegment
   * @param docId the docId of this record
   */
  void postProcessRecords(GenericRow row, int docId);

  /**
   * Retrieve information related to an upsert-enable segment virtual column for debug purpose
   *
   * @param offset the offset of the record we are trying to get the virtual columnn data for
   * @return string representation of the virtual column data information
   */
  String getVirtualColumnInfo(StreamPartitionMsgOffset offset);
}
