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
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.partition.function.ModuloPartitionFunction;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.dedup.DedupContext;
import org.apache.pinot.segment.local.dedup.DedupRecordInfo;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshManager;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.ConcurrentMapPartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/// Fault-injection coverage for issue #16316: a started realtime row is either unpublished or fully repaired.
public class IndexingFailureTest implements PinotBuffersAfterMethodCheckRule {
  private static final String TABLE_NAME = "testTable";
  private static final String INT_COL = "int_col";
  private static final String STRING_COL = "string_col";
  private static final String JSON_COL = "json_col";
  private static final String METRIC_COL = "metric_col";
  private static final String MV_COL = "mv_col";
  private static final String TIME_COL = "ts";
  private static final String PK_COL = "pk";
  private static final StreamMessageMetadata METADATA = mock(StreamMessageMetadata.class);

  private MutableSegmentImpl _mutableSegment;
  private ServerMetrics _serverMetrics;

  @BeforeMethod
  public void setup() {
    Schema schema = defaultSchema();
    _serverMetrics = mock(ServerMetrics.class);
    _mutableSegment = createSegment(schema, Set.of(), Set.of(INT_COL, STRING_COL), _serverMetrics, true);
  }

  private static MutableSegmentImpl createSegment(Schema schema, Set<String> noDictionaryColumns,
      Set<String> invertedIndexColumns, ServerMetrics serverMetrics, boolean continueOnError) {
    return MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, noDictionaryColumns, Set.of(),
        invertedIndexColumns, Map.of(JSON_COL, new JsonIndexConfig()), serverMetrics, continueOnError);
  }

  @AfterMethod
  public void tearDown() {
    if (_mutableSegment != null) {
      _mutableSegment.destroy();
      _mutableSegment = null;
    }
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
    GenericRow nullResult = _mutableSegment.getRecord(3, new GenericRow());
    assertEquals(nullResult.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("DICTIONARY-indexingError$"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
    verify(_serverMetrics, atLeastOnce()).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testFailSoftKeepsColumnLengthsAlignedAfterJsonError()
      throws IOException {
    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, 7);
    badRow.putValue(STRING_COL, "bad-json-row");
    badRow.putValue(JSON_COL, "{\"truncatedJson...");
    _mutableSegment.index(badRow, METADATA);

    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    GenericRow result = _mutableSegment.getRecord(0, new GenericRow());
    assertEquals(result.getValue(INT_COL), 7);
    assertEquals(result.getValue(STRING_COL), "bad-json-row");
    assertEquals(result.getValue(JSON_COL), "{\"truncatedJson...");

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
    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, "not-an-int");
    badRow.putValue(STRING_COL, "a");
    badRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(badRow, METADATA);

    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    GenericRow result = _mutableSegment.getRecord(0, new GenericRow());
    assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertTrue(_mutableSegment.getDataSource(INT_COL).getNullValueVector().isNull(0));
    var metadata = _mutableSegment.getDataSource(INT_COL).getDataSourceMetadata();
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
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addMetric(METRIC_COL, FieldSpec.DataType.LONG)
        .setSchemaName(TABLE_NAME)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_COL), Set.of(), Set.of(INT_COL),
            true, false, true);
    try {
      segment.index(badDimensionRow("not-an-int", 1L), METADATA);
      segment.index(badDimensionRow("also-not-an-int", 2L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);

      GenericRow defaultRow = new GenericRow();
      defaultRow.putValue(INT_COL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      defaultRow.putValue(METRIC_COL, 4L);
      segment.index(defaultRow, METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);

      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(result.getValue(METRIC_COL), 7L);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testNullValueSubstitutionMetersIncompleteRowOnce()
      throws IOException {
    Schema schema = defaultSchema();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment = createSegment(schema, Set.of(STRING_COL), Set.of(INT_COL), serverMetrics, true);
    try {
      segment.index(nullStringRow("{\"valid\": \"json\"}"), METADATA);
      segment.index(nullStringRow("{\"truncatedJson..."), METADATA);

      assertEquals(segment.getNumDocsIndexed(), 2);
      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertTrue(segment.getDataSource(STRING_COL).getNullValueVector().isNull(0));
      assertTrue(segment.getDataSource(STRING_COL).getNullValueVector().isNull(1));
      verify(serverMetrics, times(2)).addMeteredTableValue(matches("NULL_VALUE-indexingError$"),
          eq(ServerMeter.INDEXING_FAILURES), eq(1L));
      verify(serverMetrics, times(2)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testContinueOnErrorIsReadFromIngestionConfig() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .setSchemaName(TABLE_NAME)
        .build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(ingestionConfig)
        .build();

    RealtimeSegmentConfig fromTableConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema).build();
    assertTrue(fromTableConfig.isContinueOnError());

    RealtimeSegmentConfig fromIndexLoadingConfig =
        new RealtimeSegmentConfig.Builder(new IndexLoadingConfig(tableConfig, schema)).build();
    assertTrue(fromIndexLoadingConfig.isContinueOnError());

    TableConfig defaultTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    RealtimeSegmentConfig defaultFromTableConfig =
        new RealtimeSegmentConfig.Builder(defaultTableConfig, schema).build();
    assertFalse(defaultFromTableConfig.isContinueOnError());
    RealtimeSegmentConfig defaultFromIndexLoadingConfig =
        new RealtimeSegmentConfig.Builder(new IndexLoadingConfig(defaultTableConfig, schema)).build();
    assertFalse(defaultFromIndexLoadingConfig.isContinueOnError());
  }

  @Test
  public void testContinueOnErrorFalsePropagatesDictionaryFailure()
      throws IOException {
    Schema schema = defaultSchema();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment = createSegment(schema, Set.of(), Set.of(INT_COL, STRING_COL), serverMetrics, false);
    try {
      expectThrows(Exception.class, () -> segment.index(badDictionaryRow(), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(INT_COL),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      verify(serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));

      segment.index(goodRow(1, "ok"), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 2);
      assertEquals(segment.getRecord(1, new GenericRow()).getValue(INT_COL), 1);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testContinueOnErrorFalsePropagatesAddNewRowFailure()
      throws IOException {
    Schema schema = defaultSchema();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment = createSegment(schema, Set.of(), Set.of(INT_COL, STRING_COL), serverMetrics, false);
    try {
      expectThrows(Exception.class, () -> segment.index(badJsonRow(), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow published = segment.getRecord(0, new GenericRow());
      assertEquals(published.getValue(INT_COL), 7);
      assertEquals(published.getValue(STRING_COL), "bad-json-row");
      verify(serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));

      segment.index(goodRow(8, "ok"), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 2);
      GenericRow result = segment.getRecord(1, new GenericRow());
      assertEquals(result.getValue(INT_COL), 8);
      assertEquals(result.getValue(STRING_COL), "ok");
      assertEquals(segment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
          ImmutableRoaringBitmap.bitmapOf(0));
      assertEquals(segment.getDataSource(INT_COL).getInvertedIndex().getDocIds(1),
          ImmutableRoaringBitmap.bitmapOf(1));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testContinueOnErrorFalsePropagatesAggregateMetricsFailure()
      throws IOException {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addMetric(METRIC_COL, FieldSpec.DataType.LONG)
        .setSchemaName(TABLE_NAME)
        .build();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(METRIC_COL), Set.of(), Set.of(INT_COL),
            true, false, false);
    try {
      GenericRow first = new GenericRow();
      first.putValue(INT_COL, 1);
      first.putValue(METRIC_COL, 10L);
      segment.index(first, METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);

      GenericRow badRollup = new GenericRow();
      badRollup.putValue(INT_COL, 1);
      badRollup.putValue(METRIC_COL, "not-a-number");
      expectThrows(Exception.class, () -> segment.index(badRollup, METADATA));
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(METRIC_COL), 10L);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testContinueOnErrorTrueFailSoftOnDictionaryAndJsonErrors()
      throws IOException {
    Schema schema = defaultSchema();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment = createSegment(schema, Set.of(), Set.of(INT_COL, STRING_COL), serverMetrics, true);
    try {
      segment.index(badDictionaryRow(), METADATA);
      segment.index(badJsonRow(), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 2);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(INT_COL),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(segment.getRecord(1, new GenericRow()).getValue(STRING_COL), "bad-json-row");
      verify(serverMetrics, times(2)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testForwardFailureAfterAnotherColumnIsRepaired()
      throws IOException {
    _mutableSegment._indexWriteInterceptor = (column, indexType, value, docId) -> {
      if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && Integer.valueOf(42).equals(value)) {
        throw new RuntimeException("injected forward failure");
      }
    };

    GenericRow row = goodRow(42, "kept");
    _mutableSegment.index(row, METADATA);

    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    GenericRow result = _mutableSegment.getRecord(0, new GenericRow());
    assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(result.getValue(STRING_COL), "kept");
    assertEquals(result.getValue(JSON_COL), "{\"valid\": \"json\"}");
    verify(_serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testSecondaryFailureAfterForwardKeepsCanonicalValue()
      throws IOException {
    _mutableSegment._indexWriteInterceptor = (column, indexType, value, docId) -> {
      if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.inverted())) {
        throw new RuntimeException("injected inverted failure");
      }
    };

    _mutableSegment.index(goodRow(9, "secondary-miss"), METADATA);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    assertEquals(_mutableSegment.getRecord(0, new GenericRow()).getValue(STRING_COL), "secondary-miss");
    Dictionary dictionary = _mutableSegment.getDataSource(STRING_COL).getDictionary();
    int dictId = dictionary.indexOf("secondary-miss");
    assertFalse(invertedDocIds(_mutableSegment.getDataSource(STRING_COL), dictId).contains(0));
    verify(_serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
        eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
  }

  @Test
  public void testForwardFailureDoesNotLeaveOriginalValueInSecondary()
      throws IOException {
    _mutableSegment._indexWriteInterceptor = (column, indexType, value, docId) -> {
      if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && Integer.valueOf(42).equals(value)) {
        throw new RuntimeException("injected forward failure");
      }
    };

    _mutableSegment.index(goodRow(42, "order"), METADATA);
    Dictionary dictionary = _mutableSegment.getDataSource(INT_COL).getDictionary();
    int originalDictId = dictionary.indexOf(42);
    int defaultDictId = dictionary.indexOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertTrue(originalDictId >= 0);
    assertTrue(defaultDictId >= 0);
    assertFalse(invertedDocIds(_mutableSegment.getDataSource(INT_COL), originalDictId).contains(0));
    assertTrue(invertedDocIds(_mutableSegment.getDataSource(INT_COL), defaultDictId).contains(0));
    assertEquals(_mutableSegment.getRecord(0, new GenericRow()).getValue(INT_COL),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
  }

  @Test
  public void testUnrecoverableForwardRepairMakesSegmentTerminal() {
    _mutableSegment._indexWriteInterceptor = (column, indexType, value, docId) -> {
      if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward())) {
        throw new RuntimeException("forward and fallback both fail");
      }
    };

    expectThrows(Exception.class, () -> _mutableSegment.index(goodRow(1, "x"), METADATA));
    assertEquals(_mutableSegment.getNumDocsIndexed(), 0);
    assertFalse(_mutableSegment.canAddMore());
    expectThrows(IllegalStateException.class, () -> _mutableSegment.index(goodRow(2, "y"), METADATA));
    assertEquals(_mutableSegment.getNumDocsIndexed(), 0);
  }

  @Test
  public void testMultiValueNullAndSpecialIndexesShareForwardCardinality()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addMultiValueDimension(MV_COL, FieldSpec.DataType.STRING)
        .build();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(INT_COL, STRING_COL, MV_COL), Map.of(), Map.of(), false, null, true, null, null, null, serverMetrics,
        true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward())
            && Integer.valueOf(5).equals(value)) {
          throw new RuntimeException("injected forward failure");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(INT_COL, 5);
      row.putValue(STRING_COL, null);
      row.addNullValueField(STRING_COL);
      row.putValue(MV_COL, new Object[]{"red", "blue"});
      segment.index(row, METADATA);

      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(result.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertEquals(result.getValue(MV_COL), new Object[]{"red", "blue"});
      assertTrue(segment.getDataSource(STRING_COL).getNullValueVector().isNull(0));
      verify(serverMetrics, times(1)).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED), eq(1L));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testVectorIndexCardinalityMatchesForward()
      throws Exception {
    RealtimeLuceneTextIndexSearcherPool.init(1);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addMultiValueDimension("embedding", FieldSpec.DataType.FLOAT)
        .build();
    VectorIndexConfig vectorIndexConfig =
        new VectorIndexConfig(false, "HNSW", 4, 1, VectorIndexConfig.VectorDistanceFunction.COSINE,
            Map.of("vectorIndexType", "HNSW", "vectorDimension", "4", "commitDocs", "1"));
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of("embedding"),
        Set.of(), Set.of(STRING_COL), Map.of(), Map.of("embedding", vectorIndexConfig), false, null, true, null, null,
        null, mock(ServerMetrics.class), true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && "bad".equals(value)) {
          throw new RuntimeException("injected string forward failure");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(STRING_COL, "bad");
      row.putValue("embedding", new Object[]{0.1f, 0.2f, 0.3f, 0.4f});
      segment.index(row, METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(STRING_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertEquals(((Object[]) result.getValue("embedding")).length, 4);
      assertNotNull(segment.getDataSource("embedding").getVectorIndex());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testMultiColumnTextCardinalityMatchesForward()
      throws Exception {
    RealtimeLuceneTextIndexSearcherPool.init(1);
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension("body", FieldSpec.DataType.STRING)
        .build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImplWithTextIndex(schema, Set.of(),
        Set.of(INT_COL), new MultiColumnTextIndexConfig(List.of(STRING_COL, "body")), mock(ServerMetrics.class), true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && Integer.valueOf(3).equals(value)) {
          throw new RuntimeException("injected int forward failure");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(INT_COL, 3);
      row.putValue(STRING_COL, "hello");
      row.putValue("body", "world");
      segment.index(row, METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      GenericRow result = segment.getRecord(0, new GenericRow());
      assertEquals(result.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(result.getValue(STRING_COL), "hello");
      assertEquals(result.getValue("body"), "world");
      try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
        assertTrue(waitForTextMatch(segment, STRING_COL, "hello"));
        assertTrue(waitForTextMatch(segment, "body", "world"));
      }
    } finally {
      segment.destroy();
      RealtimeLuceneIndexRefreshManager.getInstance().reset();
    }
  }

  @Test
  public void testFullUpsertMetadataDoesNotPointAtUnpublishedDoc()
      throws Exception {
    Schema schema = upsertSchema();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("upsert physical write failed");
        }
      };
      expectThrows(Exception.class, () -> segment.index(upsertRow("pk-1", "sf", 10L), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0, "unpublished doc must not increment numDocs");
      assertFalse(segment.canAddMore(), "unrecoverable upsert write must terminalize the segment");
      assertFalse(segment.getValidDocIds().getMutableRoaringBitmap().contains(0),
          "validDocIds must not contain the unpublished docId");
    } finally {
      segment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testFullUpsertKeepsPreviousLocationWhenUpdateWriteFails()
      throws Exception {
    Schema schema = upsertSchema();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    try {
      segment.index(upsertRow("pk-1", "first", 10L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0));
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("upsert physical write failed");
        }
      };
      expectThrows(Exception.class, () -> segment.index(upsertRow("pk-1", "second", 20L), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 1, "failed update must not publish a new doc");
      assertFalse(segment.canAddMore(), "unrecoverable upsert write must terminalize the segment");
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0),
          "previous published location must stay valid");
      assertFalse(segment.getValidDocIds().getMutableRoaringBitmap().contains(1),
          "validDocIds must not contain the unpublished update docId");
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(STRING_COL), "first",
          "existing published row must remain readable after the failed upsert");
    } finally {
      segment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testOooUpsertKeepsPreviousLocationWhenUpdateWriteFails()
      throws Exception {
    Schema schema = upsertSchema();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDropOutOfOrderRecord(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    try {
      segment.index(upsertRow("pk-1", "first", 10L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0));
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("ooo upsert physical write failed");
        }
      };
      expectThrows(Exception.class, () -> segment.index(upsertRow("pk-1", "second", 20L), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 1, "failed in-order update must not publish a new doc");
      assertFalse(segment.canAddMore(), "unrecoverable upsert write must terminalize the segment");
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0),
          "previous published location must stay valid when dropOutOfOrderRecord is enabled");
      assertFalse(segment.getValidDocIds().getMutableRoaringBitmap().contains(1));
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(STRING_COL), "first");
    } finally {
      segment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testDroppedOooUpsertStillMetersOutOfOrder()
      throws Exception {
    ServerMetrics.deregister();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(serverMetrics));
    Schema schema = upsertSchema();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDropOutOfOrderRecord(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    try {
      segment.index(upsertRow("pk-1", "first", 20L), METADATA);
      segment.index(upsertRow("pk-1", "ooo", 10L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0));
      verify(serverMetrics, atLeastOnce()).addMeteredTableValue(eq(TABLE_NAME + "_REALTIME"),
          eq(ServerMeter.UPSERT_OUT_OF_ORDER), eq(1L));
    } finally {
      segment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
      ServerMetrics.deregister();
    }
  }

  /// The drop decision comes from an unlocked peek. If the key's segment goes away before the drop is recorded, the
  /// dropped row must not claim the doc id it never wrote; the next row lands in that doc.
  @Test
  public void testDroppedOooUpsertDoesNotClaimUnwrittenDocWhenKeyIsRemoved()
      throws Exception {
    Schema schema = upsertSchema();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .build();
    UpsertContext upsertContext = new UpsertContext.Builder().setTableConfig(tableConfig)
        .setSchema(schema)
        .setTableDataManager(mock(TableDataManager.class))
        .setPrimaryKeyColumns(List.of(PK_COL))
        .setComparisonColumns(List.of(TIME_COL))
        .setDropOutOfOrderRecord(true)
        .build();
    AtomicReference<MutableSegmentImpl> removeOnNextPeek = new AtomicReference<>();
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager(TABLE_NAME + "_REALTIME", 0, upsertContext) {
          @Override
          public boolean supportsOutOfOrderRecordCheck() {
            return true;
          }

          @Override
          public boolean isOutOfOrderRecord(RecordInfo recordInfo) {
            boolean outOfOrder = super.isOutOfOrderRecord(recordInfo);
            // Stands in for the Helix thread taking the key's segment offline right after the peek.
            MutableSegmentImpl segmentToRemove = removeOnNextPeek.getAndSet(null);
            if (segmentToRemove != null) {
              removeSegment(segmentToRemove);
            }
            return outOfOrder;
          }
        };
    MutableSegmentImpl oldSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, upsertMetadataManager, null);
    try {
      oldSegment.index(upsertRow("A", "a5", 5L), METADATA);
      assertTrue(oldSegment.getValidDocIds().getMutableRoaringBitmap().contains(0));

      removeOnNextPeek.set(oldSegment);
      segment.index(upsertRow("A", "a3", 3L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().isEmpty());

      segment.index(upsertRow("B", "b7", 7L), METADATA);
      segment.index(upsertRow("A", "a6", 6L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 2);
      assertEquals(segment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{0, 1});
      GenericRow reuse = new GenericRow();
      assertEquals(segment.getRecord(0, reuse).getValue(PK_COL), "B");
      assertEquals(reuse.getValue(STRING_COL), "b7");
      reuse.clear();
      assertEquals(segment.getRecord(1, reuse).getValue(PK_COL), "A");
      assertEquals(reuse.getValue(STRING_COL), "a6");
    } finally {
      segment.destroy();
      oldSegment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testPartialUpsertPublishesRepairedRowBeforeMetadata()
      throws Exception {
    Schema schema = upsertSchema();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setPartialUpsertStrategies(Map.of(STRING_COL, UpsertConfig.Strategy.OVERWRITE));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager upsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(), Set.of(), Map.of(), Map.of(),
            false, null, true, TIME_COL, upsertMetadataManager, null, mock(ServerMetrics.class), true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && "sf".equals(value)) {
          throw new RuntimeException("partial upsert forward failure");
        }
      };
      segment.index(upsertRow("pk-1", "sf", 10L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().contains(0));
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(STRING_COL),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    } finally {
      segment.destroy();
      upsertMetadataManager.stop();
      upsertMetadataManager.close();
    }
  }

  @Test
  public void testDedupDoesNotPublishPartialRow()
      throws Exception {
    Schema schema = upsertSchema();
    DedupConfig dedupConfig = new DedupConfig();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setDedupConfig(dedupConfig)
        .build();
    File tmpDir = new File(FileUtils.getTempDirectory(), "IndexingFailureDedup-" + System.nanoTime());
    FileUtils.forceMkdir(tmpDir);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(tmpDir);
    TableDedupMetadataManager tableDedupMetadataManager =
        TableDedupMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema, tableDataManager, null);
    PartitionDedupMetadataManager dedupMetadataManager = tableDedupMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, null, dedupMetadataManager);
    MutableSegmentImpl retrySegment = null;
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (STRING_COL.equals(column) && indexType.equals(StandardIndexes.forward())) {
          throw new RuntimeException("dedup physical write failed");
        }
      };
      expectThrows(Exception.class, () -> segment.index(upsertRow("pk-1", "sf", 10L), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertFalse(segment.canAddMore());
      assertEquals(dedupMetadataManager.getNumPrimaryKeys(), 0, "failed write must not claim the primary key");

      retrySegment =
          MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, null, dedupMetadataManager);
      retrySegment.index(upsertRow("pk-1", "sf", 10L), METADATA);
      assertEquals(retrySegment.getNumDocsIndexed(), 1, "retry must not be dropped after an unpublished failed write");
      assertEquals(dedupMetadataManager.getNumPrimaryKeys(), 1);
    } finally {
      segment.destroy();
      if (retrySegment != null) {
        retrySegment.destroy();
      }
      dedupMetadataManager.stop();
      dedupMetadataManager.close();
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  @Test
  public void testSealAndReopenAfterRepairedRow()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    File tmpDir = new File(FileUtils.getTempDirectory(), "IndexingFailureSealTest-" + System.nanoTime());
    FileUtils.forceMkdir(tmpDir);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COL, FieldSpec.DataType.JSON)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL)
        .setInvertedIndexColumns(List.of(INT_COL, STRING_COL))
        .setJsonIndexConfigs(Map.of(JSON_COL, new JsonIndexConfig()))
        .build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(INT_COL, STRING_COL), Map.of(JSON_COL, new JsonIndexConfig()), mock(ServerMetrics.class), true);
    ImmutableSegment immutableSegment = null;
    try {
      segment.index(goodRowWithTime(1, "ok", 1000L), METADATA);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (INT_COL.equals(column) && indexType.equals(StandardIndexes.forward()) && Integer.valueOf(7).equals(value)) {
          throw new RuntimeException("injected forward failure before seal");
        }
      };
      segment.index(goodRowWithTime(7, "repaired", 2000L), METADATA);
      segment._indexWriteInterceptor = null;
      segment.index(goodRowWithTime(3, "later", 3000L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 3);

      File outputDir = new File(tmpDir, "output");
      SegmentZKPropsConfig zkProps = new SegmentZKPropsConfig();
      zkProps.setStartOffset("1");
      zkProps.setEndOffset("3");
      String segmentName = "testTable__0__0__155555";
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(segment, zkProps, outputDir.getAbsolutePath(), schema,
              tableConfig.getTableName(), tableConfig, segmentName, true);
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
      assertEquals(immutableSegment.getSegmentMetadata().getTotalDocs(), 3);
      for (int docId = 0; docId < 3; docId++) {
        GenericRow sealed = immutableSegment.getRecord(docId, new GenericRow());
        assertNotNull(sealed.getValue(INT_COL));
        assertNotNull(sealed.getValue(STRING_COL));
        assertNotNull(sealed.getValue(JSON_COL));
        assertNotNull(sealed.getValue(TIME_COL));
      }
      GenericRow repaired = immutableSegment.getRecord(1, new GenericRow());
      assertEquals(repaired.getValue(INT_COL), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(repaired.getValue(STRING_COL), "repaired");

      // Representative query: inverted lookup plus a forward-index scan of every column.
      // Unequal column lengths throw IndexOutOfBoundsException here (issue #16316).
      Dictionary stringDict = immutableSegment.getDataSource(STRING_COL).getDictionary();
      int laterDictId = stringDict.indexOf("later");
      assertTrue(laterDictId >= 0);
      ImmutableRoaringBitmap laterDocs = invertedDocIds(immutableSegment.getDataSource(STRING_COL), laterDictId);
      assertTrue(laterDocs.contains(2));
      assertEquals(laterDocs.getCardinality(), 1);
      for (String column : List.of(INT_COL, STRING_COL, JSON_COL, TIME_COL)) {
        DataSource dataSource = immutableSegment.getDataSource(column);
        @SuppressWarnings("unchecked")
        ForwardIndexReader<ForwardIndexReaderContext> forward =
            (ForwardIndexReader<ForwardIndexReaderContext>) dataSource.getForwardIndex();
        Dictionary dictionary = dataSource.getDictionary();
        ForwardIndexReaderContext context = forward.createContext();
        try {
          for (int docId = 0; docId < 3; docId++) {
            if (forward.isDictionaryEncoded()) {
              int dictId = forward.getDictId(docId, context);
              assertTrue(dictId >= 0 && dictId < dictionary.length());
              assertNotNull(dictionary.get(dictId));
            } else {
              switch (forward.getStoredType()) {
                case INT:
                  forward.getInt(docId, context);
                  break;
                case LONG:
                  forward.getLong(docId, context);
                  break;
                case STRING:
                  assertNotNull(forward.getString(docId, context));
                  break;
                default:
                  fail("Unexpected stored type for column " + column + ": " + forward.getStoredType());
                  break;
              }
            }
          }
        } finally {
          if (context != null) {
            context.close();
          }
        }
      }
    } finally {
      if (immutableSegment != null) {
        immutableSegment.destroy();
      }
      segment.destroy();
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  @Test
  public void testRepairedUpsertRowKeepsMetadataAfterSealing()
      throws Exception {
    String deleteColumn = "deleted";
    Schema schema = upsertSchema();
    schema.addField(new DimensionFieldSpec(deleteColumn, DataType.BOOLEAN, true));
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(deleteColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).setUpsertConfig(upsertConfig).setNullHandlingEnabled(true).build();
    UpsertContext context = new UpsertContext.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setTableDataManager(mock(TableDataManager.class)).setPrimaryKeyColumns(List.of(PK_COL))
        .setComparisonColumns(List.of(TIME_COL)).setDeleteRecordColumn(deleteColumn).build();
    String segmentName = "testTable__0__0__155555";
    File outputDir = new File(FileUtils.getTempDirectory(), "IndexingFailureUpsertSealTest-" + System.nanoTime());
    PartitionUpsertMetadataManager liveManager =
        new ConcurrentMapPartitionUpsertMetadataManager(tableConfig.getTableName(), 0, context);
    PartitionUpsertMetadataManager rebuiltManager =
        new ConcurrentMapPartitionUpsertMetadataManager(tableConfig.getTableName(), 0, context);
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(), Map.of(), Map.of(), false, null, true, TIME_COL, liveManager, null, mock(ServerMetrics.class), true);
    ImmutableSegment sealed = null;
    try {
      GenericRow first = upsertRow("A", "original value", 1000L);
      first.putValue(deleteColumn, 0);
      segment.index(first, METADATA);
      MutableSegmentImplTestUtils.failNextForwardIndexWrite(segment, STRING_COL);
      GenericRow repaired = upsertRow("A", "repaired value", 2000L);
      repaired.putValue(deleteColumn, 0);
      segment.index(repaired, METADATA);
      GenericRow deleted = upsertRow("B", "deleted value", 3000L);
      deleted.putValue(deleteColumn, 1);
      segment.index(deleted, METADATA);
      assertEquals(segment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{1, 2});
      assertEquals(segment.getQueryableDocIds().getMutableRoaringBitmap().toArray(), new int[]{1});

      SegmentZKPropsConfig zkProps = new SegmentZKPropsConfig();
      zkProps.setStartOffset("1");
      zkProps.setEndOffset("4");
      new RealtimeSegmentConverter(segment, zkProps, outputDir.getAbsolutePath(), schema,
          tableConfig.getTableName(), tableConfig, segmentName, true).build(SegmentVersion.v3);
      sealed = ImmutableSegmentLoader.load(new File(outputDir, segmentName), ReadMode.mmap);
      rebuiltManager.addSegment(sealed);
      assertEquals(sealed.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{1, 2});
      assertEquals(sealed.getQueryableDocIds().getMutableRoaringBitmap().toArray(), new int[]{1});
      GenericRow queryable = sealed.getRecord(1, new GenericRow());
      assertEquals(queryable.getValue(PK_COL), "A");
      assertEquals(queryable.getValue(TIME_COL), 2000L);
      assertEquals(queryable.getValue(STRING_COL), schema.getFieldSpecFor(STRING_COL).getDefaultNullValue());
    } finally {
      if (sealed != null) {
        sealed.destroy();
      }
      segment.destroy();
      liveManager.stop();
      liveManager.close();
      rebuiltManager.stop();
      rebuiltManager.close();
      FileUtils.deleteQuietly(outputDir);
    }
  }

  @Test
  public void testLegacyDedupManagerStillDropsDuplicates()
      throws Exception {
    LegacyDedupMetadataManager manager = new LegacyDedupMetadataManager();
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(upsertSchema(), true, TIME_COL, null, manager);
    try {
      segment.index(upsertRow("A", "first", 10L), METADATA);
      segment.index(upsertRow("A", "duplicate", 20L), METADATA);
      assertEquals(segment.getNumDocsIndexed(), 1, "legacy duplicate result must be honored before publication");
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(STRING_COL), "first");
      assertEquals(manager.getNumPrimaryKeys(), 1);
    } finally {
      segment.destroy();
    }
  }

  @DataProvider
  public Object[][] booleanValues() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "booleanValues")
  public void testDedupClaimFailureDoesNotPublishCompletedRow(boolean becameDuplicate)
      throws Exception {
    DedupContext context = mock(DedupContext.class);
    when(context.getPrimaryKeyColumns()).thenReturn(List.of(PK_COL));
    PartitionDedupMetadataManager manager = mock(PartitionDedupMetadataManager.class);
    when(manager.getContext()).thenReturn(context);
    when(manager.supportsRecordPresenceCheck()).thenReturn(true);
    RuntimeException failure = new RuntimeException("dedup claim failed after the physical write");
    if (becameDuplicate) {
      when(manager.checkRecordPresentOrUpdate(any(), any())).thenReturn(true);
    } else {
      when(manager.checkRecordPresentOrUpdate(any(), any())).thenThrow(failure);
    }
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(upsertSchema(), true, TIME_COL, null, manager);
    try {
      RuntimeException thrown = expectThrows(RuntimeException.class,
          () -> segment.index(upsertRow("A", "unpublished", 10L), METADATA));
      if (becameDuplicate) {
        assertEquals(thrown.getMessage(), "Dedup key became present during row indexing");
      } else {
        assertSame(thrown, failure);
      }
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertFalse(segment.wasLastIndexingOperationPublished());
      assertTrue(segment.hasUnrecoverableIndexingFailure());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testUpsertClaimFailureAfterPhysicalWriteIsTerminal()
      throws Exception {
    Schema schema = upsertSchema();
    PartitionUpsertMetadataManager manager = mockUpsertManager(schema, false, null);
    RuntimeException failure = new RuntimeException("upsert claim failed after the physical write");
    when(manager.addRecord(any(), any())).thenAnswer(invocation -> {
      MutableSegmentImpl segment = invocation.getArgument(0);
      assertEquals(segment.getNumDocsIndexed(), 1, "doc count must precede the bitmap update");
      throw failure;
    });
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, manager, null);
    try {
      assertSame(expectThrows(RuntimeException.class, () -> segment.index(upsertRow("A", "a", 10L), METADATA)),
          failure);
      assertFalse(segment.wasLastIndexingOperationPublished());
      assertTrue(segment.hasUnrecoverableIndexingFailure());
    } finally {
      segment.destroy();
    }
  }

  @Test(dataProvider = "booleanValues")
  public void testLegacyUpsertManagerStillControlsOutOfOrderDropAndMark(boolean dropOutOfOrder)
      throws Exception {
    String outOfOrderColumn = "outOfOrder";
    Schema schema = upsertSchema();
    schema.addField(new DimensionFieldSpec(outOfOrderColumn, DataType.BOOLEAN, true));
    PartitionUpsertMetadataManager manager = mockUpsertManager(schema, dropOutOfOrder, outOfOrderColumn);
    when(manager.addRecord(any(), any())).thenReturn(true, false, true);
    MutableSegmentImpl segment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, manager, null);
    try {
      GenericRow newer = upsertRow("A", "a10", 10L);
      newer.putDefaultNullValue(outOfOrderColumn, 0);
      segment.index(newer, METADATA);
      GenericRow older = upsertRow("A", "a5", 5L);
      older.putDefaultNullValue(outOfOrderColumn, 0);
      segment.index(older, METADATA);

      int expectedDocs = dropOutOfOrder ? 1 : 2;
      assertEquals(segment.getNumDocsIndexed(), expectedDocs);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue(outOfOrderColumn), 0);
      if (!dropOutOfOrder) {
        GenericRow marked = segment.getRecord(1, new GenericRow());
        assertEquals(marked.getValue(outOfOrderColumn), 1);
        assertFalse(marked.isNullValue(outOfOrderColumn));
      }

      // The legacy contract calls addRecord() first. A failure after that call must prevent both publication of
      // the broken row and subsequent writes to its doc id, even though the plugin cannot roll back its claim.
      MutableSegmentImplTestUtils.failNextForwardIndexWrite(segment, PK_COL);
      expectThrows(IllegalStateException.class, () -> segment.index(upsertRow("B", "b20", 20L), METADATA));
      verify(manager, times(3)).addRecord(any(), any());
      assertEquals(segment.getNumDocsIndexed(), expectedDocs);
      assertFalse(segment.wasLastIndexingOperationPublished());
      assertTrue(segment.hasUnrecoverableIndexingFailure());
      expectThrows(IllegalStateException.class, () -> segment.index(upsertRow("C", "c30", 30L), METADATA));
      verify(manager, times(3)).addRecord(any(), any());
    } finally {
      segment.destroy();
    }
  }

  private static PartitionUpsertMetadataManager mockUpsertManager(Schema schema, boolean dropOutOfOrder,
      @Nullable String outOfOrderColumn) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).build();
    UpsertContext upsertContext = new UpsertContext.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setTableDataManager(mock(TableDataManager.class)).setPrimaryKeyColumns(List.of(PK_COL))
        .setComparisonColumns(List.of(TIME_COL)).setDropOutOfOrderRecord(dropOutOfOrder)
        .setOutOfOrderRecordColumn(outOfOrderColumn).build();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    when(manager.getContext()).thenReturn(upsertContext);
    when(manager.isAcceptingRecords()).thenReturn(true);
    when(manager.updateRecord(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
    return manager;
  }

  @DataProvider
  public Object[][] metadataFields() {
    return new Object[][]{{PK_COL, false}, {TIME_COL, false}, {"deleted", false},
        {PK_COL, true}, {TIME_COL, true}, {"deleted", true}};
  }

  @Test(dataProvider = "metadataFields")
  public void testUpsertMetadataFieldForwardFailureDoesNotPublish(String failedColumn, boolean raw)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PK_COL, DataType.STRING)
        .addSingleValueDimension("deleted", DataType.BOOLEAN)
        .addDateTime(TIME_COL, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PK_COL)).build();
    UpsertConfig config = new UpsertConfig(UpsertConfig.Mode.FULL);
    config.setDeleteRecordColumn("deleted");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).setUpsertConfig(config).setNullHandlingEnabled(true).build();
    TableUpsertMetadataManager tableManager = TableUpsertMetadataManagerFactory.create(new PinotConfiguration(),
        tableConfig, schema, mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager manager = tableManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema,
        raw ? Set.of(failedColumn) : Set.of(), Set.of(), Set.of(), Map.of(), Map.of(), false, null, true,
        TIME_COL, manager, null, mock(ServerMetrics.class), true);
    MutableSegmentImpl retry = null;
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (failedColumn.equals(column) && indexType.equals(StandardIndexes.forward())
            && !value.equals(schema.getFieldSpecFor(column).getDefaultNullValue())) {
          throw new RuntimeException("metadata field forward failure");
        }
      };
      GenericRow bad = new GenericRow();
      bad.putValue(PK_COL, "A");
      bad.putValue(TIME_COL, 20L);
      bad.putValue("deleted", 1);
      Exception failure = expectThrows(Exception.class, () -> segment.index(bad, METADATA));
      assertEquals(failure.getMessage(), "metadata field forward failure");
      assertFalse(segment.wasLastIndexingOperationPublished());
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().isEmpty());
      assertFalse(segment.canAddMore(), "metadata fields cannot be repaired under an original metadata claim");
      retry = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, manager, null);
      GenericRow good = new GenericRow();
      good.putValue(PK_COL, "A");
      good.putValue(TIME_COL, 10L);
      good.putValue("deleted", 0);
      retry.index(good, METADATA);
      assertTrue(retry.getValidDocIds().getMutableRoaringBitmap().contains(0));
      assertTrue(retry.getQueryableDocIds().getMutableRoaringBitmap().contains(0));
      assertEquals(retry.getRecord(0, new GenericRow()).getValue(TIME_COL), 10L);
    } finally {
      if (retry != null) {
        retry.destroy();
      }
      segment.destroy();
      manager.stop();
      manager.close();
    }
  }

  @Test
  public void testUpsertComparisonDictionaryFailureDoesNotClaimOriginalVersion()
      throws Exception {
    Schema schema = upsertSchema();
    UpsertConfig config = new UpsertConfig(UpsertConfig.Mode.FULL);
    config.setDropOutOfOrderRecord(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).setUpsertConfig(config).setNullHandlingEnabled(true).build();
    TableUpsertMetadataManager tableManager = TableUpsertMetadataManagerFactory.create(new PinotConfiguration(),
        tableConfig, schema, mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager manager = tableManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(), Map.of(), Map.of(), false, null, true, TIME_COL, manager, null, mock(ServerMetrics.class), true);
    MutableSegmentImpl retry = null;
    try {
      GenericRow bad = upsertRow("A", "bad version", 20L);
      bad.putValue(TIME_COL, "20");
      expectThrows(Exception.class, () -> segment.index(bad, METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertTrue(segment.getValidDocIds().getMutableRoaringBitmap().isEmpty());
      assertFalse(segment.canAddMore());
      retry = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COL, manager, null);
      retry.index(upsertRow("A", "lower valid version", 10L), METADATA);
      assertEquals(retry.getNumDocsIndexed(), 1);
      assertTrue(retry.getValidDocIds().getMutableRoaringBitmap().contains(0));
    } finally {
      if (retry != null) {
        retry.destroy();
      }
      segment.destroy();
      manager.stop();
      manager.close();
    }
  }

  @DataProvider
  public Object[][] dedupMetadataFields() {
    return new Object[][]{{PK_COL}, {TIME_COL}};
  }

  @Test(dataProvider = "dedupMetadataFields")
  public void testDedupMetadataForwardFailureDoesNotClaimSubstitutedKey(String failedColumn)
      throws Exception {
    Schema schema = upsertSchema();
    DedupConfig config = new DedupConfig();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).setDedupConfig(config).build();
    config.setMetadataTTL(20);
    config.setDedupTimeColumn(TIME_COL);
    File tmpDir = new File(FileUtils.getTempDirectory(), "dedup-key-failure-" + System.nanoTime());
    FileUtils.forceMkdir(tmpDir);
    TableDataManager dataManager = mock(TableDataManager.class);
    when(dataManager.getTableDataDir()).thenReturn(tmpDir);
    TableDedupMetadataManager tableManager = TableDedupMetadataManagerFactory.create(new PinotConfiguration(),
        tableConfig, schema, dataManager, null);
    PartitionDedupMetadataManager manager = tableManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(), Map.of(), Map.of(), false, null, false, TIME_COL, null, manager, mock(ServerMetrics.class), true);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (failedColumn.equals(column) && indexType.equals(StandardIndexes.forward())
            && !value.equals(schema.getFieldSpecFor(column).getDefaultNullValue())) {
          throw new RuntimeException("dedup key forward failure");
        }
      };
      expectThrows(Exception.class, () -> segment.index(upsertRow("A", "value", 20L), METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0);
      assertEquals(manager.getNumPrimaryKeys(), 0);
      assertFalse(segment.canAddMore());
    } finally {
      segment.destroy();
      manager.stop();
      manager.close();
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  @Test
  public void testPartitionColumnForwardFailureDoesNotPublishMismatchedMetadata()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, DataType.INT).build();
    PartitionFunction function = new ModuloPartitionFunction(3, null);
    int originalPartition = function.getPartition("42");
    assertFalse(originalPartition == function.getPartition(Integer.toString(Integer.MIN_VALUE)));
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, INT_COL,
        function, originalPartition, false);
    try {
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (indexType.equals(StandardIndexes.forward()) && Integer.valueOf(42).equals(value)) {
          throw new RuntimeException("partition column forward failure");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(INT_COL, 42);
      expectThrows(Exception.class, () -> segment.index(row, METADATA));
      assertEquals(segment.getNumDocsIndexed(), 0,
          "a substituted partition value must not be published with partitions from the original value");
      assertFalse(segment.canAddMore());
      assertEquals(segment.getDataSource(INT_COL).getDataSourceMetadata().getPartitions(), Set.of(originalPartition));
    } finally {
      segment.destroy();
    }
  }

  @DataProvider
  public Object[][] fallbackDictionaryBounds() {
    return new Object[][]{{false, Integer.MIN_VALUE}, {true, Integer.MIN_VALUE}, {false, 42}, {true, 42},
        {false, 100}, {true, 100}};
  }

  @Test(dataProvider = "fallbackDictionaryBounds")
  public void testForwardFallbackRefreshesDictionaryBounds(boolean multiValue, int defaultValue)
      throws Exception {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME);
    Schema schema = (multiValue ? builder.addMultiValueDimension(INT_COL, DataType.INT, defaultValue)
        : builder.addSingleValueDimension(INT_COL, DataType.INT, defaultValue)).build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(), false, false, true);
    try {
      AtomicBoolean failOnce = new AtomicBoolean(true);
      segment._indexWriteInterceptor = (column, indexType, value, docId) -> {
        if (indexType.equals(StandardIndexes.forward()) && failOnce.getAndSet(false)) {
          throw new RuntimeException("forward failure before fallback dictionary insertion");
        }
      };
      GenericRow row = new GenericRow();
      row.putValue(INT_COL, multiValue ? new Object[]{42, 43} : 42);
      segment.index(row, METADATA);
      assertEquals(segment.getDataSource(INT_COL).getDataSourceMetadata().getMinValue(), Math.min(42, defaultValue));
      assertEquals(segment.getDataSource(INT_COL).getDataSourceMetadata().getMaxValue(),
          Math.max(multiValue ? 43 : 42, defaultValue));
      assertEquals(segment.getDataSource(INT_COL).getDictionary().getIntValue(
          segment.getDataSource(INT_COL).getDictionary().indexOf(defaultValue)), defaultValue);
    } finally {
      segment.destroy();
    }
  }

  /// Simulates a single-writer plugin compiled against the original dedup interface.
  private static final class LegacyDedupMetadataManager implements PartitionDedupMetadataManager {
    private final Set<PrimaryKey> _keys = new HashSet<>();
    private final DedupContext _context = mock(DedupContext.class);

    private LegacyDedupMetadataManager() {
      when(_context.getPrimaryKeyColumns()).thenReturn(List.of(PK_COL));
    }

    @Override
    public DedupContext getContext() {
      return _context;
    }

    @Override
    public boolean checkRecordPresentOrUpdate(DedupRecordInfo info, IndexSegment segment) {
      return !_keys.add(info.getPrimaryKey());
    }

    @Override
    public long getNumPrimaryKeys() {
      return _keys.size();
    }

    @Override
    public void addSegment(IndexSegment segment) {
    }

    @Override
    public void preloadSegments(IndexLoadingConfig config) {
    }

    @Override
    public boolean isPreloading() {
      return false;
    }

    @Override
    public void preloadSegment(ImmutableSegment segment) {
    }

    @Override
    public void removeSegment(IndexSegment segment) {
    }

    @Override
    public void removeExpiredPrimaryKeys() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void close() {
    }
  }

  private static Schema defaultSchema() {
    return new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COL, FieldSpec.DataType.JSON)
        .setSchemaName(TABLE_NAME)
        .build();
  }

  private static Schema upsertSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PK_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PK_COL))
        .build();
  }

  private static GenericRow goodRow(int intValue, String stringValue) {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, intValue);
    row.putValue(STRING_COL, stringValue);
    row.putValue(JSON_COL, "{\"valid\": \"json\"}");
    return row;
  }

  private static GenericRow goodRowWithTime(int intValue, String stringValue, long ts) {
    GenericRow row = goodRow(intValue, stringValue);
    row.putValue(TIME_COL, ts);
    return row;
  }

  private static GenericRow badDictionaryRow() {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, "not-an-int");
    row.putValue(STRING_COL, "a");
    row.putValue(JSON_COL, "{\"valid\": \"json\"}");
    return row;
  }

  private static GenericRow badJsonRow() {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, 7);
    row.putValue(STRING_COL, "bad-json-row");
    row.putValue(JSON_COL, "{\"truncatedJson...");
    return row;
  }

  private static GenericRow badJsonRowWithTime(long ts) {
    GenericRow row = badJsonRow();
    row.putValue(TIME_COL, ts);
    return row;
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

  private static GenericRow upsertRow(String pk, String city, long ts) {
    GenericRow row = new GenericRow();
    row.putValue(PK_COL, pk);
    row.putValue(STRING_COL, city);
    row.putValue(TIME_COL, ts);
    return row;
  }

  /// Mutable inverted indexes return a generic doc-id container; realtime tests need the roaring bitmap.
  @SuppressWarnings("unchecked")
  private static ImmutableRoaringBitmap invertedDocIds(DataSource dataSource, int dictId) {
    return (ImmutableRoaringBitmap) dataSource.getInvertedIndex().getDocIds(dictId);
  }

  private static boolean waitForTextMatch(MutableSegmentImpl segment, String column, String query)
      throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + 2000;
    while (System.currentTimeMillis() < deadlineMs) {
      if (segment.getMultiColumnTextIndex().getDocIds(column, query).contains(0)) {
        return true;
      }
      Thread.sleep(20);
    }
    return false;
  }

  @Test
  public void testJsonIndexUsesParsedCache()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, 0);
    row.putValue(STRING_COL, "a");
    row.putValue(JSON_COL, "not valid json");
    row.putParsedJsonValue(JSON_COL, JsonUtils.stringToJsonNode("{\"cached\":\"value\"}"));

    _mutableSegment.index(row, METADATA);

    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("cached = 'value'"),
        ImmutableRoaringBitmap.bitmapOf(0));
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
  }

  @Test
  public void testJsonIndexUsesForwardValueForUnsupportedParsedCache()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue(INT_COL, 0);
    row.putValue(STRING_COL, "a");
    row.putValue(JSON_COL, "{\"forward\":\"value\"}");
    // A Float leaf cannot be flattened directly, so the index flattens the forward value instead.
    row.putParsedJsonValue(JSON_COL, Map.of("cached", 0.5f));

    _mutableSegment.index(row, METADATA);

    JsonIndexReader jsonIndex = _mutableSegment.getDataSource(JSON_COL).getJsonIndex();
    assertEquals(jsonIndex.getMatchingDocIds("forward = 'value'"), ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(jsonIndex.getMatchingDocIds("cached = '0.5'"), ImmutableRoaringBitmap.bitmapOf());
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
  }
}
