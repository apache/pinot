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
package org.apache.pinot.core.segment.creator.impl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.segment.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.core.util.IngestionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentIndexStatsRingBufferConsumer implements EventHandler<GenericRow>, LifecycleAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexStatsRingBufferConsumer.class);
  private RecordTransformer _transformer = null;
  private SegmentPreIndexStatsCollector _collector = null;
  public CountDownLatch _startupLatch;

  public SegmentIndexStatsRingBufferConsumer(RecordTransformer newTransformer, SegmentPreIndexStatsCollector newCollector, CountDownLatch startupLatch) {
    _transformer = newTransformer;
    _collector = newCollector;
    _startupLatch = startupLatch;
  }

  /**
   * Consumes one entry from the ring buffer. Called every time an entry becomes available.
   * @param row {@link org.apache.pinot.spi.data.readers.GenericRow} entry available in the ring buffer.
   * @param sequence index of the entry available in the ring buffer.
   * @param endOfBatch whether we reached the end of a batch processing.
   */
  public void onEvent(GenericRow row, long sequence, boolean endOfBatch) {
    try {
      if (row.getValue(GenericRow.MULTIPLE_RECORDS_KEY) != null) {
        for (Object singleRow : (Collection) row.getValue(GenericRow.MULTIPLE_RECORDS_KEY)) {
          collectRow((GenericRow) singleRow);
        }
      } 
      else {
        collectRow(row);
      }
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while gathering stats", e);
    }
  }

  private void collectRow(GenericRow row) throws Exception {
    GenericRow transformedRow = _transformer.transform(row);
    if (transformedRow != null && IngestionUtils.shouldIngestRow(row)) {
      _collector.collectRow(transformedRow);
    }
  }

  @Override
  public void onStart() {
    _startupLatch.countDown();
  }

  @Override
  public void onShutdown() { }
}
