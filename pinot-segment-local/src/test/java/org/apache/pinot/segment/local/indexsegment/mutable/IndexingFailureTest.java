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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IndexingFailureTest implements PinotBuffersAfterMethodCheckRule {
  private static final String TABLE_NAME = "testTable";
  private static final String INT_COL = "int_col";
  private static final String STRING_COL = "string_col";
  private static final String JSON_COL = "json_col";
  private static final String METRIC_COL = "metric_col";
  private static final StreamMessageMetadata METADATA = mock(StreamMessageMetadata.class);

  private MutableSegmentImpl _mutableSegment;
  private ServerMetrics _serverMetrics;

  @BeforeMethod
  public void setup() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COL, FieldSpec.DataType.JSON)
        .setSchemaName(TABLE_NAME)
        .build();
    _serverMetrics = mock(ServerMetrics.class);
    _mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
            new HashSet<>(Arrays.asList(INT_COL, STRING_COL)),
            Map.of(JSON_COL, new JsonIndexConfig()), _serverMetrics);
  }

  @AfterMethod
  public void tearDown() {
    _mutableSegment.destroy();
  }

  @Test
  public void testIndexingFailures()
      throws IOException {
    GenericRow goodRow = new GenericRow();
    goodRow.putValue(INT_COL, 0);
    goodRow.putValue(STRING_COL, "a");
    goodRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(goodRow, METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0));
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
    reset(_serverMetrics);

    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, 0);
    badRow.putValue(STRING_COL, "b");
    badRow.putValue(JSON_COL, "{\"truncatedJson...");
    _mutableSegment.index(badRow, METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 2);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0, 1));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(1),
        ImmutableRoaringBitmap.bitmapOf(1));
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("-JSON-indexingError"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
    reset(_serverMetrics);

    GenericRow anotherGoodRow = new GenericRow();
    anotherGoodRow.putValue(INT_COL, 2);
    anotherGoodRow.putValue(STRING_COL, "c");
    anotherGoodRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(anotherGoodRow, METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 3);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(1),
        ImmutableRoaringBitmap.bitmapOf(2));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(2),
        ImmutableRoaringBitmap.bitmapOf(2));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0, 2));
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
    reset(_serverMetrics);

    GenericRow nullStringRow = new GenericRow();
    nullStringRow.putValue(INT_COL, 0);
    nullStringRow.putValue(STRING_COL, null);
    nullStringRow.addNullValueField(STRING_COL);
    nullStringRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(nullStringRow, METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 4);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0, 1, 3));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0, 2, 3));
    assertTrue(_mutableSegment.getDataSource(STRING_COL).getNullValueVector().isNull(3));
    // Fail-soft (#16316): null string is completed with the field default so forward lengths stay aligned.
    GenericRow nullResult = _mutableSegment.getRecord(3, new GenericRow());
    assertEquals(nullResult.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("DICTIONARY-indexingError$"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
    // Incomplete-row meter fires once for the null-string row (dictionary error path).
    verify(_serverMetrics, atLeastOnce()).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testFailSoftKeepsColumnLengthsAlignedAfterJsonError()
      throws IOException {
    // After a secondary-index failure the row must still be fully present on every physical column so seal/query
    // lengths match numDocsIndexed (issue #16316).
    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, 7);
    badRow.putValue(STRING_COL, "bad-json-row");
    badRow.putValue(JSON_COL, "{\"truncatedJson...");
    _mutableSegment.index(badRow, METADATA);

    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    GenericRow result = _mutableSegment.getRecord(0, new GenericRow());
    assertEquals(result.getValue(INT_COL), 7);
    assertEquals(result.getValue(STRING_COL), "bad-json-row");
    // JSON forward index still holds the raw string even when the JSON secondary index fails.
    assertEquals(result.getValue(JSON_COL), "{\"truncatedJson...");

    // A subsequent good row must land on the next docId without reuse/corruption.
    GenericRow goodRow = new GenericRow();
    goodRow.putValue(INT_COL, 8);
    goodRow.putValue(STRING_COL, "ok");
    goodRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(goodRow, METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 2);
    GenericRow goodResult = _mutableSegment.getRecord(1, new GenericRow());
    assertEquals(goodResult.getValue(INT_COL), 8);
    assertEquals(goodResult.getValue(STRING_COL), "ok");

    verify(_serverMetrics, atLeastOnce()).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testDictionaryFailureIndexesDefaultValue()
      throws IOException {
    // A String in an INT column makes MutableDictionary.index() throw. The catch must index the field default instead
    // of leaving the Integer.MIN_VALUE sentinel, so the dict id matches the value that is actually stored and the
    // column min/max stay in sync with the dictionary (#16316).
    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, "not-an-int");
    badRow.putValue(STRING_COL, "a");
    badRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(badRow, METADATA);

    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    GenericRow result = _mutableSegment.getRecord(0, new GenericRow());
    assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertTrue(_mutableSegment.getDataSource(INT_COL).getNullValueVector().isNull(0));
    // min/max are only refreshed from the dictionary after a successful index, so the sentinel path left them unset.
    DataSourceMetadata metadata = _mutableSegment.getDataSource(INT_COL).getDataSourceMetadata();
    assertEquals(metadata.getMinValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(metadata.getMaxValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("DICTIONARY-indexingError$"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
    verify(_serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testDictionaryFailureKeepsAggregationKeyConsistent()
      throws IOException {
    // Metrics aggregation keys each row on the dimension dict ids (getOrCreateDocId). Integer.MIN_VALUE is not a real
    // dict id, so a failed row was keyed on something that could never match the default value stored for it, and a
    // later row legitimately carrying that default landed on a second docId with identical dimension content. Failed
    // rows deliberately share the default-value key instead (#16316).
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addMetric(METRIC_COL, FieldSpec.DataType.LONG)
        .setSchemaName(TABLE_NAME)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_COL), Set.of(), Set.of(INT_COL),
            true);
    try {
      segment.index(badDimensionRow("not-an-int", 1L), METADATA);
      segment.index(badDimensionRow("also-not-an-int", 2L), METADATA);
      // Two failed rows carrying different values now share the default-value key by design.
      assertEquals(segment.getNumDocsIndexed(), 1);

      GenericRow defaultRow = new GenericRow();
      defaultRow.putValue(INT_COL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      defaultRow.putValue(METRIC_COL, 4L);
      segment.index(defaultRow, METADATA);
      // The failed rows are keyed on what is stored for them, so the legitimate default row rolls up into the same
      // docId instead of creating a duplicate group.
      assertEquals(segment.getNumDocsIndexed(), 1);

      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(result.getValue(METRIC_COL), 7L);
    } finally {
      segment.destroy();
    }
  }

  private static GenericRow nullStringRow(String jsonValue) {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, 1);
    row.putValue(STRING_COL, null);
    row.addNullValueField(STRING_COL);
    row.putValue(JSON_COL, jsonValue);
    return row;
  }

  private static GenericRow badDimensionRow(String badValue, long metricValue) {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, badValue);
    row.putValue(METRIC_COL, metricValue);
    return row;
  }

  @Test
  public void testNullValueSubstitutionMetersIncompleteRowOnce()
      throws IOException {
    // STRING_COL has no dictionary here, so a null value first shows up in addPhysicalColumn. The substituted default
    // must mark the row incomplete on its own, and still only once when another column fails on the same row (#16316).
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COL, FieldSpec.DataType.JSON)
        .setSchemaName(TABLE_NAME)
        .build();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(STRING_COL), Set.of(), Set.of(INT_COL),
            Map.of(JSON_COL, new JsonIndexConfig()), serverMetrics);
    try {
      // Only the null substitution goes wrong on this row.
      segment.index(nullStringRow("{\"valid\": \"json\"}"), METADATA);
      // Null substitution plus a JSON index failure on the same row.
      segment.index(nullStringRow("{\"truncatedJson..."), METADATA);

      assertEquals(segment.getNumDocsIndexed(), 2);
      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertTrue(segment.getDataSource(STRING_COL).getNullValueVector().isNull(0));
      assertTrue(segment.getDataSource(STRING_COL).getNullValueVector().isNull(1));
      verify(serverMetrics, times(2)).addMeteredTableValue(matches("NULL_VALUE-indexingError$"),
          eq(ServerMeter.INDEXING_FAILURES), eq(1L));
      // Once per row: the first row is metered on the substitution alone, the second one only once despite two
      // failing columns.
      verify(serverMetrics, times(2)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
    } finally {
      segment.destroy();
    }
  }
}
