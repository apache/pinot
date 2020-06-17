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
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.io.IOException;

/**
 * create component inject to {@link org.apache.pinot.core.data.manager.SegmentDataManager} for handling extra logic for
 * other workflows other than regular append-mode ingestion.
 */
@InterfaceStability.Evolving
public interface DataManagerCallback {

  void init() throws IOException;

  /**
   * create a {@link IndexSegmentCallback} object to allow SegmentDataManager to create proper IndexSegment.
   *
   * In append-tables callback, this method will create a DefaultIndexSegmentCallback
   * In upsert-tables callback, this method will create a UpsertDataManagerCallbackImpl
   *
   * The {@link IndexSegmentCallback} will be used in the constructor of IndexSegment
   */
  IndexSegmentCallback getIndexSegmentCallback();

  /**
   * process the row after transformation in LLRealtimeSegmentDataManager.processStreamEvents(...) method
   * it happens after the GenericRow has been transformed by RecordTransformer and before it is indexed by
   * IndexSegmentImpl, to ensure we can provide other necessary data to the segment metadata.
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will attach the offset object into the GenericRow object.
   *
   * The reason we need this particular logic is that in upsert table, we need to add offset data to the physical data
   * this will help us to apply the update events from key coordinator to upsert table correctly as the offset
   * is used as the index to identify which record's virtual column we want to update
   *
   * @param row the row of newly ingested and transformed data from upstream
   * @param offset the offset of this particular pinot record
   */
  void processTransformedRow(GenericRow row, StreamPartitionMsgOffset offset);

  /**
   * process the row after indexing in LLRealtimeSegmentDataManager.processStreamEvents(...) method
   * it happens after the MutableSegmentImpl has done the indexing of the current row in its physical storage
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will emit an event to the message queue that will deliver the event to
   * key coordinator.
   *
   * This method ensures that we can emit the metadata for an new entry that pinot just indexed to its internal storage
   * and let key coordinator to be able to consume those events to process the updates correctly
   *
   * @param row the row we just index in the current segment
   * @param offset the offset associated with the current row
   */
  void postIndexProcessing(GenericRow row, StreamPartitionMsgOffset offset);

  /**
   * perform any necessary finalize operation after the consumer loop finished in LLRealtimeSegmentDataManager.consumeLoop(...)
   * method. It happens after the consumerloop exits the loop and reached the end criteria.
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will flush the queue producer to ensure all pending messages are deliverd
   * to the queue between pinot server and pinot key-coordinator
   *
   * this method will ensure that pinot server can send all events to key coordinator eventually before a segment
   * is committed. If this does not happen we might lose data in case of machine failure.
   */
  void postConsumeLoop();

  /**
   * perform any necessary clean up operation when the SegmentDataManager called its destroy() method.
   *
   * In append-tables callback, this method will do nothing
   * In upsert-tables callback, this method will notify segmentUpdater to remove any registered reference for this
   * dataManagerCallback.
   *
   * this method will ensure that segmentUpdater can keep track of which dataManager is still alive in the current pinot
   * server so it can dispatch appropriate update events to only the alive pinot data manager
   */
  void destroy();
}
