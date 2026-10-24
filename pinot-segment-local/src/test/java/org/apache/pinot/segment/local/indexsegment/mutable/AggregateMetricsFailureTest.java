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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow afterFailure = segment.getRecord(0, new GenericRow());
      assertEquals(afterFailure.getValue(METRIC_1), 10L);
      assertEquals(afterFailure.getValue(METRIC_2), 20L);
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
