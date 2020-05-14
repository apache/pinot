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
import org.apache.pinot.spi.data.readers.GenericRow;

import java.util.Map;

/**
 * component inject to {@link org.apache.pinot.core.indexsegment.IndexSegment} for handling extra logic for
 * other workflows other than regular append-mode ingestion.
 */
public interface IndexSegmentCallback {

  /**
   * initialize the callback from {@link org.apache.pinot.core.indexsegment.IndexSegment}
   * @param segmentMetadata the metadata associated with the current segment
   * @param columnIndexContainerMap mapping of necessary column name and the associate columnIndexContainer
   */
  void init(SegmentMetadata segmentMetadata, Map<String, ColumnIndexContainer> columnIndexContainerMap);

  /**
   * perform any operation from the callback for the given row after it has been processed and index
   * right now this method is a bit duplicate of
   * {@link org.apache.pinot.core.data.manager.callback.IndexSegmentCallback#postProcessRecords(GenericRow, int)}
   * However, the other method don't have information to docID and it is necessary for upsert virtual columns
   * to have these information to build the forward index and offset columns to build mapping between offset -> docId
   *
   * @param row the current pinot row we just indexed into the current IndexSegment
   * @param docId the docId of this record
   */
  void postProcessRecords(GenericRow row, int docId);

  /**
   * retrieve a information related to an upsert-enable segment virtual column for debug purpose
   * @param offset the offset of the record we are trying to get the virtual columnn data for
   * @return string representation of the virtual column data information
   */
  String getVirtualColumnInfo(long offset);
}
