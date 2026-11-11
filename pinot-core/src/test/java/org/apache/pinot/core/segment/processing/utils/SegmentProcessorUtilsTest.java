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
package org.apache.pinot.core.segment.processing.utils;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class SegmentProcessorUtilsTest {
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension("dimensionCol", DataType.STRING)
          .addMetric("metricCol", DataType.LONG)
          .addDateTime("dateTime", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

  @Test
  public void testGetFieldSpecsWithOriginalTimeField() {
    // Without the hidden field: [dateTime, dimensionCol, metricCol], group key fields are the sort fields
    Pair<List<FieldSpec>, Integer> pair = SegmentProcessorUtils.getFieldSpecs(SCHEMA, MergeType.ROLLUP, null, false);
    assertEquals(pair.getRight().intValue(), 2);
    assertFalse(pair.getLeft().stream().anyMatch(f -> f.getName().equals(TimeHandler.ORIGINAL_TIME_MS_COLUMN)));

    // With the hidden field: appended as the last sort field, after the group key fields and before the metrics
    pair = SegmentProcessorUtils.getFieldSpecs(SCHEMA, MergeType.ROLLUP, null, true);
    List<FieldSpec> fieldSpecs = pair.getLeft();
    int numSortFields = pair.getRight();
    assertEquals(numSortFields, 3);
    FieldSpec hiddenFieldSpec = fieldSpecs.get(numSortFields - 1);
    assertEquals(hiddenFieldSpec.getName(), TimeHandler.ORIGINAL_TIME_MS_COLUMN);
    assertEquals(hiddenFieldSpec.getDataType(), DataType.LONG);
    assertTrue(hiddenFieldSpec.isSingleValueField());
  }

  @Test
  public void testGetFieldSpecsWithOriginalTimeFieldAndSortOrder() {
    // With an explicit sort order, the hidden field is still the last sort field
    Pair<List<FieldSpec>, Integer> pair =
        SegmentProcessorUtils.getFieldSpecs(SCHEMA, MergeType.ROLLUP, List.of("dimensionCol"), true);
    List<FieldSpec> fieldSpecs = pair.getLeft();
    int numSortFields = pair.getRight();
    assertEquals(numSortFields, 3);
    assertEquals(fieldSpecs.get(0).getName(), "dimensionCol");
    assertEquals(fieldSpecs.get(numSortFields - 1).getName(), TimeHandler.ORIGINAL_TIME_MS_COLUMN);
  }

  @Test
  public void testGetFieldSpecsWithOriginalTimeFieldPreconditions() {
    // The hidden field is only allowed for ROLLUP
    assertThrows(IllegalArgumentException.class,
        () -> SegmentProcessorUtils.getFieldSpecs(SCHEMA, MergeType.CONCAT, null, true));
    assertThrows(IllegalArgumentException.class,
        () -> SegmentProcessorUtils.getFieldSpecs(SCHEMA, MergeType.DEDUP, null, true));

    // The schema must not contain the reserved hidden column
    Schema schemaWithReservedColumn =
        new Schema.SchemaBuilder().addSingleValueDimension("dimensionCol", DataType.STRING)
            .addSingleValueDimension(TimeHandler.ORIGINAL_TIME_MS_COLUMN, DataType.LONG)
            .addMetric("metricCol", DataType.LONG)
            .addDateTime("dateTime", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    assertThrows(IllegalArgumentException.class,
        () -> SegmentProcessorUtils.getFieldSpecs(schemaWithReservedColumn, MergeType.ROLLUP, null, true));
  }
}
