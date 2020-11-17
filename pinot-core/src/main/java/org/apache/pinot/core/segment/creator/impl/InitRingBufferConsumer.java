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

import java.util.Collection;
import org.apache.pinot.spi.data.readers.GenericRow;
import com.lmax.disruptor.EventHandler;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.segment.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.core.util.IngestionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitRingBufferConsumer implements EventHandler<GenericRow> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InitRingBufferConsumer.class);
  private RecordTransformer transformer = null;
  private SegmentPreIndexStatsCollector collector = null;

  public InitRingBufferConsumer(RecordTransformer newTransformer, SegmentPreIndexStatsCollector newCollector) {
    this.transformer = newTransformer;
    this.collector = newCollector;
  }

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
    GenericRow transformedRow = this.transformer.transform(row);
    if (transformedRow != null && IngestionUtils.shouldIngestRow(row)) {
      this.collector.collectRow(transformedRow);
    }
  }
}
