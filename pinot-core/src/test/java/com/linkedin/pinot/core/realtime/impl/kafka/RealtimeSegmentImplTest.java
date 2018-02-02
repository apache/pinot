/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Test for RealtimeSegmentImpl
 */
public class RealtimeSegmentImplTest {
  public static RealtimeSegmentImpl createRealtimeSegmentImpl(Schema schema, int sizeThresholdToFlushSegment, String tableName, String segmentName, String streamName,
      ServerMetrics serverMetrics) throws IOException {
    RealtimeSegmentDataManager segmentDataManager = mock(RealtimeSegmentDataManager.class);
    when(segmentDataManager.getSchema()).thenReturn(schema);
    when(segmentDataManager.getTableName()).thenReturn(tableName);
    when(segmentDataManager.getNoDictionaryColumns()).thenReturn(new ArrayList<String>());
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    when(segmentDataManager.getInvertedIndexColumns()).thenReturn(new ArrayList<String>());
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(any(String.class))).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(any(String.class))).thenReturn(32);
    when(segmentDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(segmentDataManager.getMemoryManager()).thenReturn(new DirectMemoryManager(segmentName));

    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.getRealtimeAvgMultiValueCount()).thenReturn(2);
    return new RealtimeSegmentImpl(serverMetrics, segmentDataManager, indexLoadingConfig, sizeThresholdToFlushSegment,
        streamName);
  }
  @Test
  public void testDropInvalidRows() throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("potato")
        .addSingleValueDimension("dimension", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .addTime("time", TimeUnit.SECONDS, FieldSpec.DataType.LONG)
        .build();

    RealtimeSegmentImpl realtimeSegment = createRealtimeSegmentImpl(schema, 100, "noTable", "noSegment",
        schema.getSchemaName(), new ServerMetrics(new MetricsRegistry()));

    // Segment should be empty
    Assert.assertEquals(realtimeSegment.getNumDocsIndexed(), 0);

    Map<String, Object> genericRowContents = new HashMap<>();
    genericRowContents.put("dimension", "potato");
    genericRowContents.put("metric", 1234L);
    genericRowContents.put("time", 4567L);
    GenericRow row = new GenericRow();
    row.init(genericRowContents);

    // Add a valid row
    boolean notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getNumDocsIndexed(), 1);

    // Add an invalid row
    genericRowContents.put("metric", null);
    notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getNumDocsIndexed(), 1);

    // Add another valid row
    genericRowContents.put("metric", 2222L);
    notFull = realtimeSegment.index(row);
    Assert.assertEquals(notFull, true);
    Assert.assertEquals(realtimeSegment.getNumDocsIndexed(), 2);
  }
}
