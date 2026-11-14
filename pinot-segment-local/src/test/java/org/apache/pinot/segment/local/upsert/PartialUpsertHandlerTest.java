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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerUtils;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.internal.util.collections.Sets;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


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
    testMerge(true, 0, true, 0L, "field4", 0L, true);
    testMerge(true, 0, false, 1, "field4", 1, false);
    // A null new value stays null (with the schema default) instead of overwriting with the default value
    testMerge(false, 3, true, 0L, "field4", 0L, true);
    testMerge(false, 3, false, 5, "field4", 5, false);
  }

  @Test
  public void testForceOverwriteStringAndMultiValue() {
    // STRING: a null new value keeps the column null with the "null" default
    testMerge(false, "a", true, "null", "field5", "null", true);
    testMerge(false, "a", false, "b", "field5", "b", false);
    // Multi-value: a null new value keeps the column null with the Object[]{default} default
    testMerge(false, new Object[]{1}, true, new Object[]{Integer.MIN_VALUE}, "field6",
        new Object[]{Integer.MIN_VALUE}, true);
    testMerge(false, new Object[]{1}, false, new Object[]{2}, "field6", new Object[]{2}, false);
  }

  @Test
  public void testForceOverwriteAsDefaultStrategy() {
    // field2 has no per-column strategy, so it takes FORCE_OVERWRITE from the default strategy
    testMerge(UpsertConfig.Strategy.FORCE_OVERWRITE, false, 3, true, 0L, "field2", 0L, true);
    testMerge(UpsertConfig.Strategy.FORCE_OVERWRITE, false, 3, false, 5, "field2", 5, false);
  }

  @Test
  public void testForceOverwriteKeepsNormalizedTimePlaceholder() {
    // The comparison column is not the time column, so the time column takes FORCE_OVERWRITE from the default
    // strategy. For a missing time value, realtime ingestion puts a valid timestamp in the row and marks it null;
    // the merge must keep both instead of replacing the timestamp with the schema default ("null").
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("pk", FieldSpec.DataType.STRING).addMetric("version", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS")
        .setPrimaryKeyColumns(List.of("pk")).build();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.FORCE_OVERWRITE);
    upsertConfig.setComparisonColumns(List.of("version"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setTimeColumnName("day").setNullHandlingEnabled(true).setUpsertConfig(upsertConfig).build();
    PartialUpsertHandler handler = new PartialUpsertHandler(tableConfig, schema, List.of("version"), upsertConfig);
    LazyRow prevRecord = mockLazyRow(mock(LazyRow.class), Map.of("pk", "pk1", "version", 1L, "day", "20261101"));

    GenericRow missingDay = initGenericRow(new GenericRow(), Map.of("pk", "pk1", "version", 2L));
    missingDay.putDefaultNullValue("day", "20261113");
    handler.merge(prevRecord, missingDay, new HashMap<>());
    assertEquals(missingDay.getValue("day"), "20261113");
    assertTrue(missingDay.isNullValue("day"));

    GenericRow newDay = initGenericRow(new GenericRow(), Map.of("pk", "pk1", "version", 2L, "day", "20261112"));
    handler.merge(prevRecord, newDay, new HashMap<>());
    assertEquals(newDay.getValue("day"), "20261112");
    assertFalse(newDay.isNullValue("day"));
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
        Map.of("pk", "pk1", "field1", 3L, "field2", "inc", "hoursSinceEpoch", 2L));
    LazyRow prevRecord = mock(LazyRow.class);
    mockLazyRow(prevRecord, Map.of("pk", "pk1", "field1", 5L, "field2", "set", "hoursSinceEpoch", 2L));
    GenericRow expectedRecord = initGenericRow(new GenericRow(),
        Map.of("pk", "pk1", "field1", 8L, "field2", "inc", "hoursSinceEpoch", 2L));

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

  @Test
  public void testPostUpsertTransformRunsAfterMerge() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("pk",
            FieldSpec.DataType.STRING)
        .addSingleValueDimension("firstName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("lastName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("fullName", FieldSpec.DataType.STRING)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(List.of("pk")).build();

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumns(List.of("hoursSinceEpoch"));
    upsertConfig.setPostPartialUpsertTransformConfigs(
        List.of(new TransformConfig("fullName", "concat(firstName,lastName)")));
    TableConfig tableConfig = createTableConfig(schema, upsertConfig);
    List<RecordTransformer> postUpdateTransformers =
        RecordTransformerUtils.getPostPartialUpsertTransformers(tableConfig, schema);
    assertNotNull(postUpdateTransformers);
    PartialUpsertHandler handler =
        new PartialUpsertHandler(tableConfig, schema, List.of("hoursSinceEpoch"), upsertConfig);

    LazyRow previousRow = mock(LazyRow.class);
    when(previousRow.getColumnNames()).thenReturn(
        Set.of("pk", "firstName", "lastName", "fullName", "hoursSinceEpoch"));
    when(previousRow.getValue("pk")).thenReturn("pk1");
    when(previousRow.getValue("firstName")).thenReturn("Alice");
    when(previousRow.getValue("lastName")).thenReturn(null);
    when(previousRow.getValue("fullName")).thenReturn(schema.getFieldSpecFor("fullName").getDefaultNullValue());
    when(previousRow.getValue("hoursSinceEpoch")).thenReturn(1L);

    GenericRow newRecord = new GenericRow();
    newRecord.putValue("pk", "pk1");
    newRecord.putValue("lastName", "Smith");
    newRecord.putValue("hoursSinceEpoch", 2L);
    newRecord.putDefaultNullValue("firstName", schema.getFieldSpecFor("firstName").getDefaultNullValue());
    newRecord.putDefaultNullValue("fullName", schema.getFieldSpecFor("fullName").getDefaultNullValue());

    GenericRow transformOnlyRow = new GenericRow();
    transformOnlyRow.putValue("firstName", "Alice");
    transformOnlyRow.putValue("lastName", "Smith");
    transformOnlyRow.putDefaultNullValue("fullName", schema.getFieldSpecFor("fullName").getDefaultNullValue());
    for (RecordTransformer transformer : postUpdateTransformers) {
      transformer.transform(transformOnlyRow);
    }
    assertEquals(transformOnlyRow.getValue("fullName"), "AliceSmith");

    handler.merge(previousRow, newRecord, new HashMap<>());
    assertEquals(newRecord.getValue("firstName"), "Alice");
    assertEquals(newRecord.getValue("lastName"), "Smith");
    assertEquals(FunctionEvaluatorFactory.getExpressionEvaluator("concat(firstName,lastName)").evaluate(newRecord),
        "AliceSmith");
    assertEquals(newRecord.getValue("fullName"), "AliceSmith");
    assertFalse(newRecord.isNullValue("fullName"));
  }

  public void testMerge(boolean isPreviousNull, Object previousValue, boolean isNewNull, Object newValue,
      String columnName, Object expectedValue, boolean isExpectedNull) {
    testMerge(UpsertConfig.Strategy.IGNORE, isPreviousNull, previousValue, isNewNull, newValue, columnName,
        expectedValue, isExpectedNull);
  }

  private void testMerge(UpsertConfig.Strategy defaultStrategy, boolean isPreviousNull, Object previousValue,
      boolean isNewNull, Object newValue, String columnName, Object expectedValue, boolean isExpectedNull) {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG).addMetric("field2", FieldSpec.DataType.LONG)
        .addMetric("field4", FieldSpec.DataType.LONG)
        .addSingleValueDimension("field5", FieldSpec.DataType.STRING)
        .addMultiValueDimension("field6", FieldSpec.DataType.INT)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.OVERWRITE);
    partialUpsertStrategies.put("field4", UpsertConfig.Strategy.FORCE_OVERWRITE);
    partialUpsertStrategies.put("field5", UpsertConfig.Strategy.FORCE_OVERWRITE);
    partialUpsertStrategies.put("field6", UpsertConfig.Strategy.FORCE_OVERWRITE);

    try (MockedConstruction<PinotSegmentColumnReader> reader = mockConstruction(PinotSegmentColumnReader.class,
        (mockReader, context) -> {
          when(mockReader.isNull(1)).thenReturn(isPreviousNull);
          when(mockReader.getValue(1)).thenReturn(previousValue);
        })) {
      UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
      upsertConfig.setPartialUpsertStrategies(partialUpsertStrategies);
      upsertConfig.setDefaultPartialUpsertStrategy(defaultStrategy);
      TableConfig tableConfig = createTableConfig(schema, upsertConfig);
      PartialUpsertHandler handler =
          spy(new PartialUpsertHandler(tableConfig, schema, List.of("hoursSinceEpoch"),
              upsertConfig));

      ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
      when(segment.getColumnNames()).thenReturn(
          Sets.newSet("field1", "field2", "field4", "field5", "field6", "hoursSinceEpoch"));
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

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    upsertConfig.setPartialUpsertMergerClass("org.apache.pinot.segment.local.upsert.CustomPartialUpsertRowMerger");

    try (MockedStatic<PartialUpsertMergerFactory> partialUpsertMergerFactory = mockStatic(
        PartialUpsertMergerFactory.class)) {
      when(PartialUpsertMergerFactory.getPartialUpsertMerger(Arrays.asList("pk"), Arrays.asList("hoursSinceEpoch"),
          upsertConfig)).thenReturn(customMerger);
      TableConfig tableConfig = createTableConfig(schema, upsertConfig);
      PartialUpsertHandler handler =
          new PartialUpsertHandler(tableConfig, schema, List.of("hoursSinceEpoch"), upsertConfig);
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

  private TableConfig createTableConfig(Schema schema, UpsertConfig upsertConfig) {
    String tableName = schema.getSchemaName();
    if (StringUtils.isEmpty(tableName)) {
      tableName = "testTable";
    }
    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName)
        .setTimeColumnName("hoursSinceEpoch").setNullHandlingEnabled(true).setUpsertConfig(upsertConfig).build();
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
