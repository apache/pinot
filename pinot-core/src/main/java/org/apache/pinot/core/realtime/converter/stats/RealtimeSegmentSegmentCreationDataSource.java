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
package org.apache.pinot.core.realtime.converter.stats;

import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentRecordReader;
import org.apache.pinot.core.segment.creator.SegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.core.segment.creator.StatsCollectorConfig;


/**
 * Segment creation data source that is based on an in-memory realtime segment.
 */
public class RealtimeSegmentSegmentCreationDataSource implements SegmentCreationDataSource {
  private final MutableSegmentImpl _realtimeSegment;
  private final RealtimeSegmentRecordReader _realtimeSegmentRecordReader;
  private final Schema _schema;

  public RealtimeSegmentSegmentCreationDataSource(MutableSegmentImpl realtimeSegment,
      RealtimeSegmentRecordReader realtimeSegmentRecordReader, Schema schema) {
    _realtimeSegment = realtimeSegment;
    _realtimeSegmentRecordReader = realtimeSegmentRecordReader;
    _schema = schema;
  }

  @Override
  public SegmentPreIndexStatsContainer gatherStats(StatsCollectorConfig statsCollectorConfig) {
    if (!statsCollectorConfig.getSchema().equals(_schema)) {
      throw new RuntimeException("Incompatible schemas used for conversion and extraction");
    }

    return new RealtimeSegmentStatsContainer(
            _realtimeSegment, _realtimeSegmentRecordReader);
  }

  @Override
  public RecordReader getRecordReader() {
    return _realtimeSegmentRecordReader;
  }
}
