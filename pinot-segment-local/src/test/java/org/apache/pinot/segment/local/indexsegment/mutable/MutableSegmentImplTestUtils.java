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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MutableSegmentImplTestUtils {
  private MutableSegmentImplTestUtils() {
  }

  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment__0__0__155555";
  private static final String STEAM_NAME = "testStream";

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        aggregateMetrics, false);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics,
      boolean nullHandlingEnabled) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        aggregateMetrics, nullHandlingEnabled, null, null, null);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics,
      boolean nullHandlingEnabled, UpsertConfig upsertConfig, String timeColumnName,
      PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);

    UpsertConfig.Mode upsertMode = upsertConfig == null ? UpsertConfig.Mode.NONE : upsertConfig.getMode();
    String comparisonColumn = upsertConfig == null ? null : upsertConfig.getComparisonColumn();
    UpsertConfig.HashFunction hashFunction =
        upsertConfig == null ? UpsertConfig.HashFunction.NONE : upsertConfig.getHashFunction();
    RealtimeSegmentConfig realtimeSegmentConfig =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(TABLE_NAME_WITH_TYPE).setSegmentName(SEGMENT_NAME)
            .setStreamName(STEAM_NAME).setSchema(schema).setTimeColumnName(timeColumnName).setCapacity(100000)
            .setAvgNumMultiValues(2).setNoDictionaryColumns(noDictionaryColumns)
            .setVarLengthDictionaryColumns(varLengthDictionaryColumns).setInvertedIndexColumns(invertedIndexColumns)
            .setSegmentZKMetadata(new SegmentZKMetadata(SEGMENT_NAME))
            .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME)).setStatsHistory(statsHistory)
            .setAggregateMetrics(aggregateMetrics).setNullHandlingEnabled(nullHandlingEnabled).setUpsertMode(upsertMode)
            .setUpsertComparisonColumn(comparisonColumn).setHashFunction(hashFunction)
            .setPartitionUpsertMetadataManager(partitionUpsertMetadataManager).build();
    return new MutableSegmentImpl(realtimeSegmentConfig, null);
  }
}
