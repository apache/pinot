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
package org.apache.pinot.core.indexsegment.mutable;

import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.spi.data.Schema;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MutableSegmentImplTestUtils {
  private MutableSegmentImplTestUtils() {
  }

  private static final String STEAM_NAME = "testStream";
  private static final String SEGMENT_NAME = "testSegment";

  public static MutableSegmentImpl createMutableSegmentImpl(@Nonnull Schema schema,
      @Nonnull Set<String> noDictionaryColumns, @Nonnull Set<String> varLengthDictionaryColumns,
      @Nonnull Set<String> invertedIndexColumns, boolean aggregateMetrics) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        aggregateMetrics, false);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(@Nonnull Schema schema,
      @Nonnull Set<String> noDictionaryColumns, @Nonnull Set<String> varLengthDictionaryColumns,
      @Nonnull Set<String> invertedIndexColumns, boolean aggregateMetrics, boolean nullHandlingEnabled) {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);

    RealtimeSegmentConfig realtimeSegmentConfig =
        new RealtimeSegmentConfig.Builder().setSegmentName(SEGMENT_NAME).setStreamName(STEAM_NAME).setSchema(schema)
            .setCapacity(100000).setAvgNumMultiValues(2).setNoDictionaryColumns(noDictionaryColumns)
            .setVarLengthDictionaryColumns(varLengthDictionaryColumns).setInvertedIndexColumns(invertedIndexColumns)
            .setRealtimeSegmentZKMetadata(new RealtimeSegmentZKMetadata())
            .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME)).setStatsHistory(statsHistory)
            .setAggregateMetrics(aggregateMetrics).setNullHandlingEnabled(nullHandlingEnabled).build();
    return new MutableSegmentImpl(realtimeSegmentConfig);
  }
}
