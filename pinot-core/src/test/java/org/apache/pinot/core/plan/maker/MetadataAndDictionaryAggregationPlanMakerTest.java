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
package org.apache.pinot.core.plan.maker;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.upsert.ConcurrentMapPartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.upsert.UpsertUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class MetadataAndDictionaryAggregationPlanMakerTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_201711219_20171120";
  // A small segment with fully predictable data so aggregation results can be asserted exactly. It holds the 10 rows
  // metricCol = i, intCol = i + 10, bytesCol = new byte[]{(byte) i} for i = 1..10, with every row duplicated (20 rows
  // total).
  private static final String PREDICTABLE_TABLE_NAME = "predictableTable";
  private static final String PREDICTABLE_SEGMENT_NAME = "predictableTable_segment";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "MetadataAndDictionaryAggregationPlanMakerTest");
  private static final InstancePlanMakerImplV2 PLAN_MAKER = new InstancePlanMakerImplV2();

  private IndexSegment _indexSegment;
  private IndexSegment _upsertIndexSegment;
  private IndexSegment _predictableSegment;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setSegmentTimeValueCheck(false);
    ingestionConfig.setRowTimeValueCheck(false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setTimeColumnName("daysSinceEpoch")
        .setInvertedIndexColumns(List.of("column6", "column7", "column11", "column17", "column18"))
        .setIngestionConfig(ingestionConfig)
        .build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    buildPredictableSegment();
  }

  /**
   * Builds a segment with fully predictable data so that aggregation results can be asserted exactly. It contains the
   * 10 rows where row {@code i} (for {@code i = 1..10}) has {@code metricCol = i}, {@code intCol = i + 10} and
   * {@code bytesCol = new byte[]{(byte) i}}, and every row is duplicated (20 rows total).
   */
  private void buildPredictableSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(PREDICTABLE_TABLE_NAME)
        .addMetric("metricCol", FieldSpec.DataType.INT)
        .addMetric("intCol", FieldSpec.DataType.INT)
        .addSingleValueDimension("bytesCol", FieldSpec.DataType.BYTES)
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(PREDICTABLE_TABLE_NAME).build();

    List<GenericRow> records = new ArrayList<>(20);
    // Add each row twice.
    for (int n = 0; n < 2; n++) {
      for (int i = 1; i <= 10; i++) {
        GenericRow record = new GenericRow();
        record.putValue("metricCol", i);
        record.putValue("intCol", i + 10);
        record.putValue("bytesCol", new byte[]{(byte) i});
        records.add(record);
      }
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(PREDICTABLE_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(PREDICTABLE_SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _upsertIndexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _predictableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, PREDICTABLE_SEGMENT_NAME), ReadMode.heap);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(INDEX_DIR);
    UpsertContext upsertContext = new UpsertContext.Builder()
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(List.of("column6"))
        .setComparisonColumns(List.of("daysSinceEpoch"))
        .build();
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager("testTable_REALTIME", 0, upsertContext);
    ((ImmutableSegmentImpl) _upsertIndexSegment).enableUpsert(upsertMetadataManager,
        new ThreadSafeMutableRoaringBitmap(), null);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
    _upsertIndexSegment.offload();
    _upsertIndexSegment.destroy();
    _predictableSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test(dataProvider = "testPlanMakerDataProvider")
  public void testPlanMaker(String query, Class<? extends Operator<?>> operatorClass,
      Class<? extends Operator<?>> upsertOperatorClass) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_indexSegment), queryContext).run();
    assertTrue(operatorClass.isInstance(operator));

    SegmentContext segmentContext = new SegmentContext(_upsertIndexSegment);
    segmentContext.setQueryableDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(_upsertIndexSegment));
    Operator<?> upsertOperator = PLAN_MAKER.makeSegmentPlanNode(segmentContext, queryContext).run();
    assertTrue(upsertOperatorClass.isInstance(upsertOperator));
  }

  /**
   * Verifies the partial metadata-based aggregation path. When a query mixes a metadata-eligible function (MAX) with a
   * non-eligible one (SUM), the plan uses an {@link AggregationOperator} that pre-aggregates the eligible function from
   * metadata while scanning the rest. Before this feature, a non-scan based operator was used only when <em>all</em>
   * functions were metadata eligible; a mixed query would have scanned every function.
   * <p>
   * To prove the eligible function is actually served from metadata (and not scanned), the column dictionary is
   * overridden to report a bogus max value that does not exist in the data. MAX equals the bogus value only if the
   * metadata path is taken (a scan would return the true max of 10), while SUM equals the true scanned sum of
   * {@code metricCol} ((1 + 2 + ... + 10) over the two row copies = 110).
   */
  @Test
  public void testPartialMetadataBasedAggregationServesEligibleFromMetadata() {
    int bogusMax = 999_999_999;
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "select max(metricCol), sum(metricCol) from " + PREDICTABLE_TABLE_NAME);

    // Override only metricCol's dictionary max value; everything else delegates to the real segment.
    DataSource realDataSource = _predictableSegment.getDataSource("metricCol", queryContext.getSchema());
    Dictionary dictionaryWithBogusMax = mock(Dictionary.class, delegatesTo(realDataSource.getDictionary()));
    doReturn(bogusMax).when(dictionaryWithBogusMax).getMaxVal();
    DataSource dataSourceWithBogusMax = mock(DataSource.class, delegatesTo(realDataSource));
    doReturn(dictionaryWithBogusMax).when(dataSourceWithBogusMax).getDictionary();
    IndexSegment segment = mock(IndexSegment.class, delegatesTo(_predictableSegment));
    doReturn(dataSourceWithBogusMax).when(segment).getDataSource(eq("metricCol"), any());

    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(segment), queryContext).run();
    // A mixed query must use the (partial) AggregationOperator, not the fully non-scan based operator.
    assertTrue(operator instanceof AggregationOperator);

    AggregationResultsBlock resultsBlock = (AggregationResultsBlock) operator.nextBlock();
    List<Object> results = resultsBlock.getResults();
    assertNotNull(results);
    // MAX is served from the (overridden) dictionary metadata, so the bogus value proves the metadata path was used.
    assertEquals(((Number) results.get(0)).doubleValue(), (double) bogusMax);
    // SUM is scanned, so the result is the true sum of metricCol: (1 + 2 + ... + 10) over the two row copies = 110.
    assertEquals(((Number) results.get(1)).doubleValue(), 110.0);
  }

  /**
   * {@code distinctcount} over a dictionary-encoded column is resolved entirely from the dictionary (its cardinality is
   * the number of distinct values), so it must plan to the fully non-scan {@link NonScanBasedAggregationOperator}
   * without a mock. {@code metricCol} holds the 10 distinct values 1..10 across 20 (duplicated) rows, so the distinct
   * count is 10 even though the segment has 20 docs.
   */
  @Test
  public void testDistinctCountResolvedFromDictionary() {
    // Sanity check that the fixture actually has duplicate rows, so distinct count < total docs is a meaningful result.
    assertEquals(_predictableSegment.getSegmentMetadata().getTotalDocs(), 20);

    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("select distinctcount(metricCol) from " + PREDICTABLE_TABLE_NAME);

    Operator<?> operator =
        PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_predictableSegment), queryContext).run();
    // The column is dictionary-encoded, so DISTINCTCOUNT is resolved from the dictionary without scanning the segment.
    assertTrue(operator instanceof NonScanBasedAggregationOperator);

    AggregationResultsBlock resultsBlock = (AggregationResultsBlock) operator.nextBlock();
    List<Object> results = resultsBlock.getResults();
    assertNotNull(results);
    // The intermediate result is the set of distinct dictionary values: metricCol has 10 distinct values (1..10),
    // fewer than the 20 total docs, proving DISTINCTCOUNT counts distinct values rather than rows.
    assertEquals(((Set<?>) results.get(0)).size(), 10);
  }

  /**
   * Tests for aggregations that cannot be resolved from dictionary/metadata and must therefore fall back to
   * the scan-based {@link AggregationOperator}, still returning the correct scanned result:
   * <ul>
   *   <li>a single non-resolvable aggregation with no filter ({@code sum(metricCol)}): the partial metadata path
   *   evaluates eligibility per function, and when none is resolvable it must fall back to a full scan rather than
   *   emitting a (partial) non-scan operator with zero resolved functions;</li>
   *   <li>an aggregation over an expression argument ({@code max(add(metricCol, intCol))}): the argument is not a plain
   *   column reference, so it cannot be resolved from dictionary/metadata.</li>
   * </ul>
   */
  @Test(dataProvider = "nonResolvableScanQueries")
  public void testNonResolvableAggregationFallsToScan(String description, String query, double expectedResult) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    Operator<?> operator =
        PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_predictableSegment), queryContext).run();
    assertTrue(operator instanceof AggregationOperator, description);

    AggregationResultsBlock resultsBlock = (AggregationResultsBlock) operator.nextBlock();
    List<Object> results = resultsBlock.getResults();
    assertNotNull(results, description);
    assertEquals(((Number) results.get(0)).doubleValue(), expectedResult, description);
  }

  @DataProvider(name = "nonResolvableScanQueries")
  public Object[][] nonResolvableScanQueries() {
    return new Object[][]{
        // SUM is not resolvable from metadata and is scanned: (1 + 2 + ... + 10) over the two row copies = 110.
        {"single non-resolvable aggregation", "select sum(metricCol) from " + PREDICTABLE_TABLE_NAME, 110.0},
        // add(metricCol, intCol) = i + (i + 10) is an expression argument, so it is scanned; max over i = 1..10 is 30.
        {"aggregation over expression argument",
            "select max(add(metricCol, intCol)) from " + PREDICTABLE_TABLE_NAME, 30.0}
    };
  }

  /**
   * MIN/MAX derive their result numerically from the column min/max, which is only valid for numeric columns.
   * For a non-numeric (BYTES) column the dictionary stores raw values that cannot be
   * parsed as numbers, so {@code max(bytesCol)} and {@code min(bytesCol)} must fall back to the scan-based
   * {@link AggregationOperator} rather than being (wrongly) resolved from the dictionary by a
   * {@link NonScanBasedAggregationOperator}. The scan path in turn throws a
   * {@link BadQueryRequestException} for non-numeric aggregation.
   */
  @Test
  public void testMinMaxOnNonNumericColumnFallsToScan() {
    assertNotNull(_predictableSegment.getDataSourceNullable("bytesCol").getDictionary());

    for (String function : List.of("max", "min")) {
      String query = "select " + function + "(bytesCol) from " + PREDICTABLE_TABLE_NAME;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      Operator<?> operator =
          PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_predictableSegment), queryContext).run();
      // MIN/MAX on a non-numeric column must fall back to the scan-based AggregationOperator, not the metadata path.
      assertTrue(operator instanceof AggregationOperator, query);
      assertFalse(operator instanceof NonScanBasedAggregationOperator, query);

      // Running the scan path proves it still executes and correctly rejects the non-numeric aggregation rather than
      // producing a wrong result.
      BadQueryRequestException exception = expectThrows(BadQueryRequestException.class, operator::nextBlock);
      assertTrue(exception.getMessage().contains("Cannot compute " + function + " for non-numeric type: BYTES"),
          exception.getMessage());
    }
  }

  @DataProvider(name = "testPlanMakerDataProvider")
  public Object[][] testPlanMakerDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    // Selection
    entries.add(new Object[]{
        "select * from testTable", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // Selection
    entries.add(new Object[]{
        "select column1,column5 from testTable", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // Selection with filter
    entries.add(new Object[]{
        "select * from testTable where daysSinceEpoch > 100", SelectionOnlyOperator.class, SelectionOnlyOperator.class
    });
    // COUNT from metadata (via non-scan-based aggregation because it is an all-match)
    entries.add(new Object[]{
        "select count(*) from testTable", NonScanBasedAggregationOperator.class, FastFilteredCountOperator.class
    });
    // COUNT from metadata with a non-match-all filter, fast filtered count is to be used
    entries.add(new Object[]{
        "select count(*) from testTable where column1 <= 10", FastFilteredCountOperator.class,
        FastFilteredCountOperator.class
    });
    // COUNT from metadata with match all filter
    entries.add(new Object[]{
        "select count(*) from testTable where column1 > 10", NonScanBasedAggregationOperator.class,
        FastFilteredCountOperator.class
    });
    // MIN/MAX from dictionary
    entries.add(new Object[]{
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // MIN/MAX from dictionary with match all filter
    entries.add(new Object[]{
        "select max(daysSinceEpoch),min(daysSinceEpoch) from testTable where column1 > 10",
        NonScanBasedAggregationOperator.class, AggregationOperator.class
    });
    // MINMAXRANGE from dictionary
    entries.add(new Object[]{
        "select minmaxrange(daysSinceEpoch) from testTable", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // MINMAXRANGE from dictionary with match all filter
    entries.add(new Object[]{
        "select minmaxrange(daysSinceEpoch) from testTable where column1 > 10", NonScanBasedAggregationOperator.class,
        AggregationOperator.class
    });
    // Aggregation
    entries.add(new Object[]{
        "select sum(column1) from testTable", AggregationOperator.class, AggregationOperator.class
    });
    entries.add(new Object[]{
        "select count(*), sum(column1) from testTable", AggregationOperator.class, AggregationOperator.class
    });
    // Aggregation group-by
    entries.add(new Object[]{
        "select sum(column1) from testTable group by daysSinceEpoch", GroupByOperator.class, GroupByOperator.class
    });
    // COUNT from metadata, MIN from dictionary
    entries.add(new Object[]{
        "select count(*),min(column17) from testTable", NonScanBasedAggregationOperator.class, AggregationOperator.class
    });
    // Aggregation group-by
    entries.add(new Object[]{
        "select count(*),min(daysSinceEpoch) from testTable group by daysSinceEpoch", GroupByOperator.class,
        GroupByOperator.class
    });

    return entries.toArray(new Object[entries.size()][]);
  }
}
