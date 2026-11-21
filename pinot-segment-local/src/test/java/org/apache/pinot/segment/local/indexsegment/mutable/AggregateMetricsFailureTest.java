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

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies that in-place metric rollups are all-or-nothing from a reader's perspective (issue #16316).
public class AggregateMetricsFailureTest implements PinotBuffersAfterMethodCheckRule {
  private static final String DIM = "dim";
  private static final String METRIC_1 = "metric1";
  private static final String METRIC_2 = "metric2";
  private static final StreamMessageMetadata METADATA = mock(StreamMessageMetadata.class);

  @Test
  public void testSecondMetricCommitFailureRestoresFirstMetric()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggFailure")
        .addSingleValueDimension(DIM, DataType.INT)
        .addMetric(METRIC_1, DataType.LONG)
        .addMetric(METRIC_2, DataType.LONG)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_1, METRIC_2), Set.of(), Set.of(DIM),
            true, false, false);
    try {
      GenericRow first = metricRow(1, 10L, 20L);
      segment.index(first, METADATA);
      assertTrue(segment.wasLastIndexingOperationPublished());
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(METRIC_1), 10L);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(METRIC_2), 20L);

      AtomicBoolean firstRowDone = new AtomicBoolean(true);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (firstRowDone.get() && METRIC_2.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("second metric commit failed");
        }
      };

      GenericRow rollup = metricRow(1, 3L, 4L);
      RuntimeException thrown = expectThrows(RuntimeException.class, () -> segment.index(rollup, METADATA));
      assertTrue(thrown.getMessage().contains("second metric"));
      assertFalse(segment.wasLastIndexingOperationPublished(), "a rolled-back update must not report publication");
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow afterFailure = segment.getRecord(0, new GenericRow());
      assertEquals(afterFailure.getValue(METRIC_1), 10L);
      assertEquals(afterFailure.getValue(METRIC_2), 20L);
      assertTrue(segment.canAddMore());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testFailedAggregateRestoreAlwaysThrows()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggRestoreFailure")
        .addSingleValueDimension(DIM, DataType.INT)
        .addMetric(METRIC_1, DataType.LONG)
        .addMetric(METRIC_2, DataType.LONG)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_1, METRIC_2), Set.of(), Set.of(DIM),
            true, false, true);
    try {
      segment.index(metricRow(1, 10L, 20L), METADATA);
      AtomicBoolean firstRowDone = new AtomicBoolean(true);
      AtomicBoolean restorePhase = new AtomicBoolean(false);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (!firstRowDone.get() || !indexType.equals(StandardIndexes.forward())) {
          return;
        }
        if (METRIC_2.equals(column)) {
          restorePhase.set(true);
          throw new RuntimeException("second metric commit failed");
        }
        if (restorePhase.get() && METRIC_1.equals(column)) {
          throw new RuntimeException("metric restore failed");
        }
      };
      RuntimeException thrown =
          expectThrows(RuntimeException.class, () -> segment.index(metricRow(1, 3L, 4L), METADATA));
      assertTrue(thrown.getMessage().contains("second metric") || thrown.getMessage().contains("restore"),
          thrown.getMessage());
      assertFalse(thrown.getMessage().contains("segment full"), thrown.getMessage());
      assertFalse(segment.canAddMore(), "failed restore must terminalize the segment");
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testFailedRestoreWithSameExceptionStillTerminalizesSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggSharedRestoreFailure")
        .addSingleValueDimension(DIM, DataType.INT)
        .addMetric(METRIC_1, DataType.LONG).addMetric(METRIC_2, DataType.LONG).build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema,
        Set.of(METRIC_1, METRIC_2), Set.of(), Set.of(), true, false, true);
    try {
      segment.index(metricRow(1, 10L, 20L), METADATA);
      RuntimeException failure = new RuntimeException("shared commit and restore failure");
      AtomicBoolean restoring = new AtomicBoolean(false);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (!indexType.equals(StandardIndexes.forward())) {
          return;
        }
        if (METRIC_2.equals(column)) {
          restoring.set(true);
          throw failure;
        }
        if (restoring.get() && METRIC_1.equals(column)) {
          throw failure;
        }
      };
      RuntimeException thrown = expectThrows(RuntimeException.class, () -> segment.index(metricRow(1, 3L, 4L),
          METADATA));
      assertSame(thrown, failure);
      assertFalse(segment.canAddMore(), "failed rollback must terminalize even when a plugin reuses an exception");
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testSecondMetricCommitFailureIsFailSoftWhenContinueOnError()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggFailureSoft")
        .addSingleValueDimension(DIM, DataType.INT)
        .addMetric(METRIC_1, DataType.LONG)
        .addMetric(METRIC_2, DataType.LONG)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_1, METRIC_2), Set.of(), Set.of(DIM),
            true, false, true);
    try {
      segment.index(metricRow(1, 10L, 20L), METADATA);
      AtomicBoolean firstRowDone = new AtomicBoolean(true);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (firstRowDone.get() && METRIC_2.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("second metric commit failed");
        }
      };
      segment.index(metricRow(1, 3L, 4L), METADATA);
      assertFalse(segment.wasLastIndexingOperationPublished());
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow afterFailure = segment.getRecord(0, new GenericRow());
      assertEquals(afterFailure.getValue(METRIC_1), 10L);
      assertEquals(afterFailure.getValue(METRIC_2), 20L);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testAggregationKeyForwardFailureIsTerminal()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggKeyFailure")
        .addSingleValueDimension(DIM, DataType.INT).addMetric(METRIC_1, DataType.LONG).build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_1),
        Set.of(), Set.of(), true, false, true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (DIM.equals(column) && indexType.equals(StandardIndexes.forward()) && Integer.valueOf(42).equals(value)) {
          throw new RuntimeException("aggregation key forward failure");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(DIM, 42);
      row.putValue(METRIC_1, 1L);
      expectThrows(Exception.class, () -> segment.index(row, METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertFalse(segment.canAddMore());
      segment._indexWriteInterceptor = null;
      row.putValue(METRIC_1, 2L);
      expectThrows(IllegalStateException.class, () -> segment.index(row, METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0, "later input cannot merge into a substituted aggregation key");
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testMutableBytesAggregateRollbackRestoresOriginalBytes()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("aggBytesFailure")
        .addSingleValueDimension(DIM, DataType.INT)
        .addMetric(METRIC_1, DataType.BYTES).addMetric(METRIC_2, DataType.LONG).build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema,
        Set.of(METRIC_1, METRIC_2), Set.of(), Set.of(),
        List.of(new AggregationConfig(METRIC_1, "AVG(input)"), new AggregationConfig(METRIC_2, "SUM(input)")), true);
    try {
      GenericRow row = new GenericRow();
      row.putValue(DIM, 1);
      row.putValue("input", 10L);
      segment.index(row, METADATA);
      byte[] before = ((byte[]) segment.getRecord(0, new GenericRow()).getValue(METRIC_1)).clone();
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (METRIC_2.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("second metric commit failed after mutable AVG");
        }
      };
      row.putValue("input", 20L);
      segment.index(row, METADATA);
      GenericRow after = segment.getRecord(0, new GenericRow());
      assertEquals((byte[]) after.getValue(METRIC_1), before, "rollback must restore bytes captured before AVG.apply");
      assertEquals(after.getValue(METRIC_2), 10L);
      assertTrue(segment.canAddMore());
      segment._indexWriteInterceptor = null;
      segment.index(row, METADATA);
      GenericRow afterRetry = segment.getRecord(0, new GenericRow());
      AvgPair average = AvgPair.fromBytes((byte[]) afterRetry.getValue(METRIC_1));
      assertEquals(average.getSum(), 30D);
      assertEquals(average.getCount(), 2L);
      assertEquals(afterRetry.getValue(METRIC_2), 30L);
    } finally {
      segment.destroy();
    }
  }

  private static GenericRow metricRow(int dim, long metric1, long metric2) {
    GenericRow row = new GenericRow();
    row.putValue(DIM, dim);
    row.putValue(METRIC_1, metric1);
    row.putValue(METRIC_2, metric2);
    return row;
  }
}
