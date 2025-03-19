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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.internal.util.collections.Sets;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
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
  public void testForceOverwrite() {
    testMerge(true, 0, true, 0, "field4", 0, true);
    testMerge(true, 0, false, 1, "field4", 1, false);
    testMerge(false, 0, true, 1, "field4", 1, true);
    testMerge(false, 3, false, 5, "field4", 5, false);
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

  @Test
  public void testCustomPartialUpsertMergerWithNonNullResult() {
    GenericRow newRecord = initGenericRow(new GenericRow(),
        ImmutableMap.of("pk", "pk1", "field1", 3L, "field2", "inc", "hoursSinceEpoch", 2L));
    LazyRow prevRecord = mock(LazyRow.class);
    mockLazyRow(prevRecord, ImmutableMap.of("pk", "pk1", "field1", 5L, "field2", "set", "hoursSinceEpoch", 2L));
    GenericRow expectedRecord = initGenericRow(new GenericRow(),
        ImmutableMap.of("pk", "pk1", "field1", 8L, "field2", "inc", "hoursSinceEpoch", 2L));

    testCustomMerge(prevRecord, newRecord, expectedRecord, getCustomMerger());
  }

  @Test
  public void testCustomPartialUpsertMergerWithNullResult() {
    Map newRowData = new HashMap(Map.of("pk", "pk1", "field1", 3L, "field2", "reset"));
    newRowData.put("hoursSinceEpoch", null); // testing null comparison column
    GenericRow newRecord = initGenericRow(new GenericRow(), newRowData);
    LazyRow prevRecord = mock(LazyRow.class);
    mockLazyRow(prevRecord,
        Map.of("pk", "pk1", "field1", 5L, "field2", "set", "field3", new Integer[]{0}, "hoursSinceEpoch", 2L));
    Map<String, Object> expectedData = new HashMap<>(
        Map.of("pk", "pk1", "field2", "reset", "hoursSinceEpoch", 2L));
    expectedData.put("field1", Long.MIN_VALUE);
    GenericRow expectedRecord = initGenericRow(new GenericRow(), expectedData);
    expectedRecord.addNullValueField("field1");
    expectedRecord.putDefaultNullValue("field3", new Object[]{Integer.MIN_VALUE});

    testCustomMerge(prevRecord, newRecord, expectedRecord, getCustomMerger());
  }

  public void testMerge(boolean isPreviousNull, Object previousValue, boolean isNewNull, Object newValue,
      String columnName, Object expectedValue, boolean isExpectedNull) {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG).addMetric("field2", FieldSpec.DataType.LONG)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.OVERWRITE);
    partialUpsertStrategies.put("field4", UpsertConfig.Strategy.FORCE_OVERWRITE);

    try (MockedConstruction<PinotSegmentColumnReader> reader = mockConstruction(PinotSegmentColumnReader.class,
        (mockReader, context) -> {
          when(mockReader.isNull(1)).thenReturn(isPreviousNull);
          when(mockReader.getValue(1)).thenReturn(previousValue);
        })) {
      UpsertConfig upsertConfig = new UpsertConfig();
      upsertConfig.setPartialUpsertStrategies(partialUpsertStrategies);
      upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.IGNORE);
      PartialUpsertHandler handler =
          spy(new PartialUpsertHandler(schema, Collections.singletonList("hoursSinceEpoch"), upsertConfig));

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
      handler.merge(prevRecord, row, new HashMap<>());
      assertEquals(row.getValue(columnName), expectedValue);
      assertEquals(row.isNullValue(columnName), isExpectedNull);
    }
  }

  private void testCustomMerge(LazyRow prevRecord, GenericRow newRecord, GenericRow expectedRecord,
      PartialUpsertMerger customMerger) {

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("field2", FieldSpec.DataType.STRING)
        .addMultiValueDimension("field3", FieldSpec.DataType.INT)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();

    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    upsertConfig.setPartialUpsertMergerClass("org.apache.pinot.segment.local.upsert.CustomPartialUpsertRowMerger");

    try (MockedStatic<PartialUpsertMergerFactory> partialUpsertMergerFactory = mockStatic(
        PartialUpsertMergerFactory.class)) {
      when(PartialUpsertMergerFactory.getPartialUpsertMerger(Arrays.asList("pk"), Arrays.asList("hoursSinceEpoch"),
          upsertConfig)).thenReturn(customMerger);
      PartialUpsertHandler handler =
          new PartialUpsertHandler(schema, Collections.singletonList("hoursSinceEpoch"), upsertConfig);
      HashMap<String, Object> reuseMergerResult = new HashMap<>();
      handler.merge(prevRecord, newRecord, reuseMergerResult);
      assertEquals(newRecord, expectedRecord);
    }
  }

  public PartialUpsertMerger getCustomMerger() {
    return (previousRow, newRow, resultHolder) -> {
      if ((newRow.getValue("field2")).equals("set")) {
        // use default merger (overwrite)
        return;
      }
      if ((newRow.getValue("field2")).equals("inc")) {
        resultHolder.put("field1", (Long) previousRow.getValue("field1") + (Long) newRow.getValue("field1"));
        return;
      }
      if ((newRow.getValue("field2")).equals("reset")) {
        resultHolder.put("field1", null);
        resultHolder.put("field3", null);
      }
    };
  }

  private LazyRow mockLazyRow(LazyRow prevRecord, Map<String, Object> values) {
    reset(prevRecord);
    when(prevRecord.getColumnNames()).thenReturn(values.keySet());
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      when(prevRecord.getValue(entry.getKey())).thenReturn(entry.getValue());
    }
    return prevRecord;
  }

  private GenericRow initGenericRow(GenericRow genericRow, Map<String, Object> values) {
    genericRow.clear();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String field = entry.getKey();
      Object value = entry.getValue();
      genericRow.putValue(field, value);
      if (value == null) {
        genericRow.addNullValueField(field);
      }
    }
    return genericRow;
  }
}
