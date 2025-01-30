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
package org.apache.pinot.core.query.executor;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.timeseries.TimeSeriesOperatorUtils;
import org.apache.pinot.core.operator.transform.function.TimeSeriesBucketTransformFunction;
import org.apache.pinot.core.query.aggregation.function.TimeSeriesAggregationFunction;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class QueryExecutorTest {
  private static final String AVRO_DATA_PATH = "data/sampleEatsData30k.avro";
  private static final String EMPTY_JSON_DATA_PATH = "data/test_empty_data.json";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTest");
  private static final String RAW_TABLE_NAME = "sampleEatsData";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_SEGMENTS_TO_GENERATE = 2;
  private static final int NUM_EMPTY_SEGMENTS_TO_GENERATE = 2;
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);
  private static final String TIME_SERIES_LANGUAGE_NAME = "QueryExecutorTest";
  private static final String TIME_SERIES_TIME_COL_NAME = "orderCreatedTimestamp";
  private static final Long TIME_SERIES_TEST_START_TIME = 1726228400L;

  private final List<ImmutableSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);
  private final List<String> _segmentNames = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);

  private QueryExecutor _queryExecutor;

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));

    // Set up the segments
    FileUtils.deleteQuietly(TEMP_DIR);
    assertTrue(TEMP_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    Schema schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(avroFile);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    File tableDataDir = new File(TEMP_DIR, OFFLINE_TABLE_NAME);
    int i = 0;
    for (; i < NUM_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfig(avroFile, FileFormat.AVRO, tableDataDir, RAW_TABLE_NAME,
              tableConfig, schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
      Assert.assertFalse(ingestionSchemaValidator.getDataTypeMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getMultiValueStructureMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getMissingPinotColumnResult().isMismatchDetected());
      _indexSegments.add(ImmutableSegmentLoader.load(new File(tableDataDir, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }
    resourceUrl = getClass().getClassLoader().getResource(EMPTY_JSON_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File jsonFile = new File(resourceUrl.getFile());
    for (; i < NUM_SEGMENTS_TO_GENERATE + NUM_EMPTY_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfig(jsonFile, FileFormat.JSON, tableDataDir, RAW_TABLE_NAME,
              tableConfig, schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      _indexSegments.add(ImmutableSegmentLoader.load(new File(tableDataDir, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }

    // Mock the instance data manager
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    TableDataManagerProvider tableDataManagerProvider = new DefaultTableDataManagerProvider();
    tableDataManagerProvider.init(instanceDataManagerConfig, mock(HelixManager.class), new SegmentLocks(), null, null);
    TableDataManager tableDataManager = tableDataManagerProvider.getTableDataManager(tableConfig);
    tableDataManager.start();
    for (ImmutableSegment indexSegment : _indexSegments) {
      tableDataManager.addSegment(indexSegment);
    }
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(OFFLINE_TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = CommonsConfigurationUtils.fromFile(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, ServerMetrics.get());

    // Setup time series builder factory
    TimeSeriesBuilderFactoryProvider.registerSeriesBuilderFactory(TIME_SERIES_LANGUAGE_NAME,
        new SimpleTimeSeriesBuilderFactory());
  }

  @Test
  public void testCountQuery() {
    String query = "SELECT COUNT(*) FROM " + OFFLINE_TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    assertEquals(((AggregationResultsBlock) instanceResponse.getResultsBlock()).getResults().get(0), 60000L);
  }

  @Test
  public void testSumQuery() {
    String query = "SELECT SUM(orderItemCount) FROM " + OFFLINE_TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    assertEquals(((AggregationResultsBlock) instanceResponse.getResultsBlock()).getResults().get(0), 120306.0);
  }

  @Test
  public void testMaxQuery() {
    String query = "SELECT MAX(orderAmount) FROM " + OFFLINE_TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    assertEquals(((AggregationResultsBlock) instanceResponse.getResultsBlock()).getResults().get(0), 999.0);
  }

  @Test
  public void testMinQuery() {
    String query = "SELECT MIN(orderAmount) FROM " + OFFLINE_TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    assertEquals(((AggregationResultsBlock) instanceResponse.getResultsBlock()).getResults().get(0), 0.0);
  }

  @Test
  public void testTimeSeriesSumQuery() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(TIME_SERIES_TEST_START_TIME, Duration.ofHours(2), 2);
    QueryContext queryContext = getQueryContextForTimeSeries("orderAmount", timeBuckets, 0L,
        new AggInfo("SUM", false, Collections.emptyMap()), Collections.emptyList());
    ServerQueryRequest serverQueryRequest =
        new ServerQueryRequest(queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(serverQueryRequest, QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    TimeSeriesBlock timeSeriesBlock = TimeSeriesOperatorUtils.buildTimeSeriesBlock(timeBuckets,
        (AggregationResultsBlock) instanceResponse.getResultsBlock());
    assertEquals(timeSeriesBlock.getSeriesMap().size(), 1);
    assertNull(timeSeriesBlock.getSeriesMap().values().iterator().next().get(0).getDoubleValues()[0]);
    assertEquals(timeSeriesBlock.getSeriesMap().values().iterator().next().get(0).getDoubleValues()[1], 29885544.0);
  }

  @Test
  public void testTimeSeriesMaxQuery() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(TIME_SERIES_TEST_START_TIME, Duration.ofMinutes(1), 100);
    QueryContext queryContext = getQueryContextForTimeSeries("orderItemCount", timeBuckets, 0L,
        new AggInfo("MAX", false, Collections.emptyMap()), List.of("cityName"));
    ServerQueryRequest serverQueryRequest =
        new ServerQueryRequest(queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(serverQueryRequest, QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof GroupByResultsBlock);
    GroupByResultsBlock resultsBlock = (GroupByResultsBlock) instanceResponse.getResultsBlock();
    TimeSeriesBlock timeSeriesBlock = TimeSeriesOperatorUtils.buildTimeSeriesBlock(timeBuckets, resultsBlock);
    assertEquals(5, timeSeriesBlock.getSeriesMap().size());
    // For any city, say "New York", the max order item count should be 4
    boolean foundNewYork = false;
    for (var listOfTimeSeries : timeSeriesBlock.getSeriesMap().values()) {
      assertEquals(listOfTimeSeries.size(), 1);
      TimeSeries timeSeries = listOfTimeSeries.get(0);
      if (timeSeries.getTagValues()[0].equals("New York")) {
        assertFalse(foundNewYork, "Found multiple time-series for New York");
        foundNewYork = true;
        Optional<Double> maxValue =
            Arrays.stream(timeSeries.getDoubleValues()).filter(Objects::nonNull).max(Comparator.naturalOrder());
        assertTrue(maxValue.isPresent());
        assertEquals(maxValue.get().longValue(), 4L);
      }
    }
    assertTrue(foundNewYork, "Did not find the expected time-series");
  }

  @Test
  public void testTimeSeriesMinQuery() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(TIME_SERIES_TEST_START_TIME, Duration.ofMinutes(1), 100);
    QueryContext queryContext = getQueryContextForTimeSeries("orderItemCount", timeBuckets, 0L,
        new AggInfo("MIN", false, Collections.emptyMap()), List.of("cityName"));
    ServerQueryRequest serverQueryRequest =
        new ServerQueryRequest(queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(serverQueryRequest, QUERY_RUNNERS);
    assertTrue(instanceResponse.getResultsBlock() instanceof GroupByResultsBlock);
    TimeSeriesBlock timeSeriesBlock = TimeSeriesOperatorUtils.buildTimeSeriesBlock(timeBuckets,
        (GroupByResultsBlock) instanceResponse.getResultsBlock());
    assertEquals(5, timeSeriesBlock.getSeriesMap().size());
    // For any city, say "Chicago", the min order item count should be 0
    boolean foundChicago = false;
    for (var listOfTimeSeries : timeSeriesBlock.getSeriesMap().values()) {
      assertEquals(listOfTimeSeries.size(), 1);
      TimeSeries timeSeries = listOfTimeSeries.get(0);
      if (timeSeries.getTagValues()[0].equals("Chicago")) {
        assertFalse(foundChicago, "Found multiple time-series for Chicago");
        foundChicago = true;
        Optional<Double> minValue =
            Arrays.stream(timeSeries.getDoubleValues()).filter(Objects::nonNull).min(Comparator.naturalOrder());
        assertTrue(minValue.isPresent());
        assertEquals(minValue.get().longValue(), 0L);
      }
    }
    assertTrue(foundChicago, "Did not find the expected time-series");
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private ServerQueryRequest getQueryRequest(InstanceRequest instanceRequest) {
    return new ServerQueryRequest(instanceRequest, ServerMetrics.get(), System.currentTimeMillis());
  }

  private QueryContext getQueryContextForTimeSeries(String valueExpression, TimeBuckets timeBuckets, long offsetSeconds,
      AggInfo aggInfo, List<String> groupBy) {
    List<ExpressionContext> groupByExpList = groupBy.stream().map(RequestContextUtils::getExpression)
        .collect(Collectors.toList());
    ExpressionContext timeExpression = TimeSeriesBucketTransformFunction.create(TIME_SERIES_TIME_COL_NAME,
        TimeUnit.SECONDS, timeBuckets, offsetSeconds);
    ExpressionContext aggregateExpr = TimeSeriesAggregationFunction.create(TIME_SERIES_LANGUAGE_NAME, valueExpression,
        timeExpression, timeBuckets, aggInfo);
    QueryContext.Builder builder = new QueryContext.Builder();
    builder.setTableName(OFFLINE_TABLE_NAME);
    builder.setAliasList(Collections.emptyList());
    builder.setSelectExpressions(List.of(aggregateExpr));
    // We pass in null to group-by exp to get AggregationResultsBlock in response to test both group-by and agg paths.
    builder.setGroupByExpressions(groupByExpList.isEmpty() ? null : groupByExpList);
    builder.setLimit(Integer.MAX_VALUE);
    return builder.build();
  }
}
