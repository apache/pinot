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
package org.apache.pinot.segment.local.upsert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.mockito.MockedConstruction;
import org.mockito.internal.util.collections.Sets;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PartialUpsertHandlerTest {

  @Test
  public void testOverwrite() {
    testMerge(true, 2, true, 2, "field1", 2, true);
    testMerge(true, 2, false, 8, "field1", 8, false);
    testMerge(false, 8, true, 2, "field1", 8, false);
    testMerge(false, 3, false, 5, "field1", 5, false);
  }

  @Test
  public void testNonOverwrite() {
    testMerge(true, 2, true, 2, "field2", 2, true);
    testMerge(true, 2, false, 8, "field2", 8, false);
    testMerge(false, 8, true, 2, "field2", 8, false);
    testMerge(false, 3, false, 5, "field2", 3, false);
  }

  @Test
  public void testComparisonColumn() {
    // Even though the default strategy is IGNORE, we do not apply the mergers to comparison columns
    testMerge(true, 0, true, 0, "hoursSinceEpoch", 0, true);
    testMerge(true, 0, false, 8, "hoursSinceEpoch", 8, false);
    testMerge(false, 8, true, 0, "hoursSinceEpoch", 8, false);
    testMerge(false, 2, false, 8, "hoursSinceEpoch", 8, false);
  }

  public void testMerge(boolean isPreviousNull, Object previousValue, boolean isNewNull, Object newValue,
      String columnName, Object expectedValue, boolean isExpectedNull) {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG).addMetric("field2", FieldSpec.DataType.LONG)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.OVERWRITE);

    try (MockedConstruction<PinotSegmentColumnReader> reader = mockConstruction(PinotSegmentColumnReader.class,
        (mockReader, context) -> {
          when(mockReader.isNull(1)).thenReturn(isPreviousNull);
          when(mockReader.getValue(1)).thenReturn(previousValue);
        })) {
      PartialUpsertHandler handler =
          spy(new PartialUpsertHandler(schema, partialUpsertStrategies, UpsertConfig.Strategy.IGNORE,
              Collections.singletonList("hoursSinceEpoch")));

      ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
      when(segment.getColumnNames()).thenReturn(Sets.newSet("field1", "field2", "hoursSinceEpoch"));
      LazyRow prevRecord = new LazyRow();
      prevRecord.init(segment, 1);

      GenericRow row = new GenericRow();
      if (isNewNull) {
        row.putDefaultNullValue(columnName, newValue);
      } else {
        row.putValue(columnName, newValue);
      }
      handler.merge(prevRecord, row);
      assertEquals(row.getValue(columnName), expectedValue);
      assertEquals(row.isNullValue(columnName), isExpectedNull);
    }
  }
}
