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
package org.apache.pinot.segment.local.segment.creator;

import org.apache.pinot.common.Utils;
import org.apache.pinot.segment.local.indexsegment.mutable.IntermediateSegment;
import org.apache.pinot.segment.local.segment.readers.IntermediateSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IntermediateSegmentSegmentCreationDataSource implements SegmentCreationDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntermediateSegmentSegmentCreationDataSource.class);
  private final IntermediateSegment _intermediateSegment;
  private final IntermediateSegmentRecordReader _intermediateSegmentRecordReader;

  public IntermediateSegmentSegmentCreationDataSource(IntermediateSegmentRecordReader intermediateSegmentRecordReader) {
    _intermediateSegmentRecordReader = intermediateSegmentRecordReader;
    _intermediateSegment = _intermediateSegmentRecordReader.getIntermediateSegment();
  }

  @Override
  public SegmentPreIndexStatsContainer gatherStats(StatsCollectorConfig statsCollectorConfig) {
    return new IntermediateSegmentStatsContainer(_intermediateSegment);
  }

  @Override
  public RecordReader getRecordReader() {
    try {
      _intermediateSegmentRecordReader.rewind();
    } catch (Exception e) {
      LOGGER.error("Caught exception while rewinding record reader", e);
      Utils.rethrowException(e);
    }
    return _intermediateSegmentRecordReader;
  }
}
