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
package org.apache.pinot.segment.local.realtime.converter.stats;

import java.io.File;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.readers.RecordReader;


/**
 * Segment creation data source that is based on an in-memory realtime segment.
 */
public class RealtimeSegmentSegmentCreationDataSource implements SegmentCreationDataSource {
  private final MutableSegment _mutableSegment;
  private final PinotSegmentRecordReader _recordReader;

  public RealtimeSegmentSegmentCreationDataSource(MutableSegment mutableSegment,
      PinotSegmentRecordReader recordReader) {
    _mutableSegment = mutableSegment;
    _recordReader = recordReader;
  }

  @Override
  public SegmentPreIndexStatsContainer gatherStats(StatsCollectorConfig statsCollectorConfig) {
    return new RealtimeSegmentStatsContainer(_mutableSegment, _recordReader.getSortedDocIds(), statsCollectorConfig);
  }

  @Override
  public RecordReader getRecordReader() {
    return _recordReader;
  }

  /**
   * Returns the consumer directory of the realtime segment
   */
  public File getConsumerDir() {
    return _mutableSegment.getConsumerDir();
  }
}
