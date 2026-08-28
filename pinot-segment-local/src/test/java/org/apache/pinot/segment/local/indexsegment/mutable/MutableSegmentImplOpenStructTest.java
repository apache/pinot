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

import java.io.File;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.openstruct.MutableOpenStructDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Covers the [MutableSegmentImpl] precondition that an OPEN_STRUCT column cannot be turned into a
/// DataSource unless `open_struct_index` is enabled for it. The index is enabled by default
/// ([OpenStructIndexConfig#DEFAULT]), so the guard is only reachable when a table config explicitly
/// disables it — without the guard that misconfiguration would surface as a ClassCastException on a
/// null/foreign index far from its cause.
public class MutableSegmentImplOpenStructTest {
  private static final String TABLE_NAME_WITH_TYPE =
      TableNameBuilder.REALTIME.tableNameWithType("openStructGuardTest");
  private static final String SEGMENT_NAME = "openStructGuardTest__0__0__" + UUID.randomUUID();
  private static final File TEMP_DIR =
      new File(System.getProperty("java.io.tmpdir"), MutableSegmentImplOpenStructTest.class.getSimpleName());

  private static MutableSegmentImpl createSegment(boolean openStructIndexEnabled) {
    Schema schema = new Schema();
    schema.addField(new ComplexFieldSpec("event", DataType.OPEN_STRUCT, true, Map.of()));

    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);

    RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(TABLE_NAME_WITH_TYPE)
        .setSegmentName(SEGMENT_NAME)
        .setStreamName("openStructGuardTest")
        .setSchema(schema)
        .setCapacity(1000)
        .setAvgNumMultiValues(2)
        .setIndex("event", StandardIndexes.openStruct(),
            openStructIndexEnabled ? OpenStructIndexConfig.DEFAULT : OpenStructIndexConfig.DISABLED)
        .setSegmentZKMetadata(new SegmentZKMetadata(SEGMENT_NAME))
        .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME))
        .setStatsHistory(statsHistory)
        .setConsumerDir(new File(TEMP_DIR, UUID.randomUUID().toString()).getAbsolutePath())
        .build();
    return new MutableSegmentImpl(config, null);
  }

  @Test
  public void testToDataSourceThrowsWhenOpenStructIndexDisabled() {
    MutableSegmentImpl segment = createSegment(false);
    try {
      IllegalStateException e =
          expectThrows(IllegalStateException.class, () -> segment.getDataSource("event"));
      assertTrue(e.getMessage().contains("open_struct_index"), "unexpected message: " + e.getMessage());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testToDataSourceSucceedsWhenOpenStructIndexEnabled() {
    MutableSegmentImpl segment = createSegment(true);
    try {
      DataSource dataSource = segment.getDataSource("event");
      assertNotNull(dataSource);
      assertTrue(dataSource instanceof MutableOpenStructDataSource);
    } finally {
      segment.destroy();
    }
  }
}
