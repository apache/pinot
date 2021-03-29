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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.query.comparison.QueryComparison;
import org.apache.pinot.tools.query.comparison.SegmentInfoProvider;
import org.apache.pinot.tools.query.comparison.StarTreeQueryGenerator;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for Star-Tree based indexes.
 * <ul>
 *   <li>
 *     Set up the Pinot cluster and create two tables, one with default indexes, one with star tree indexes
 *   </li>
 *   <li>
 *     Send queries to both the tables and assert that results match
 *   </li>
 *   <li>
 *     Query to reference table is sent with TOP 10000, and the comparator ensures that response from star tree is
 *     contained within the reference response. This is to avoid false failures when groups with same value are
 *     truncated due to TOP N
 *   </li>
 * </ul>
 */
public class StarTreeClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected static final String DEFAULT_TABLE_NAME = "myTable";
  protected static final String STAR_TREE_TABLE_NAME = "myStarTable";
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_single_value_columns.schema";
  private static final int NUM_STAR_TREE_DIMENSIONS = 5;
  private static final int NUM_STAR_TREE_METRICS = 5;
  private static final List<AggregationFunctionType> AGGREGATION_FUNCTION_TYPES = Arrays
      .asList(AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE,
          AggregationFunctionType.DISTINCTCOUNTBITMAP);
  private static final int NUM_QUERIES_TO_GENERATE = 100;

  private String _currentTable;
  private StarTreeQueryGenerator _starTree1QueryGenerator;
  private StarTreeQueryGenerator _starTree2QueryGenerator;

  @Override
  protected String getTableName() {
    return _currentTable;
  }

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  // NOTE: Star-Tree and SegmentInfoProvider does not work on no-dictionary dimensions
  @Override
  protected List<String> getNoDictionaryColumns() {
    return Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    File defaultTableSegmentDir = new File(_segmentDir, DEFAULT_TABLE_NAME);
    File defaultTableTarDir = new File(_tarDir, DEFAULT_TABLE_NAME);
    File starTreeTableSegmentDir = new File(_segmentDir, STAR_TREE_TABLE_NAME);
    File starTreeTableTarDir = new File(_tarDir, STAR_TREE_TABLE_NAME);
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir, defaultTableSegmentDir, defaultTableTarDir,
        starTreeTableSegmentDir, starTreeTableTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    _currentTable = DEFAULT_TABLE_NAME;
    TableConfig defaultTableConfig = createOfflineTableConfig();
    addTableConfig(defaultTableConfig);

    // Randomly pick some dimensions and metrics for star-trees
    List<String> starTree1Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
    List<String> starTree2Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
    List<String> allDimensions = new ArrayList<>(schema.getDimensionNames());
    Collections.shuffle(allDimensions);
    for (int i = 0; i < NUM_STAR_TREE_DIMENSIONS; i++) {
      starTree1Dimensions.add(allDimensions.get(2 * i));
      starTree2Dimensions.add(allDimensions.get(2 * i + 1));
    }
    List<String> starTree1Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
    List<String> starTree2Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
    List<String> allMetrics = new ArrayList<>(schema.getMetricNames());
    Collections.shuffle(allMetrics);
    for (int i = 0; i < NUM_STAR_TREE_METRICS; i++) {
      starTree1Metrics.add(allMetrics.get(2 * i));
      starTree2Metrics.add(allMetrics.get(2 * i + 1));
    }
    _currentTable = STAR_TREE_TABLE_NAME;
    TableConfig starTreeTableConfig = createOfflineTableConfig();
    starTreeTableConfig.getIndexingConfig().setStarTreeIndexConfigs(Arrays
        .asList(getStarTreeIndexConfig(starTree1Dimensions, starTree1Metrics),
            getStarTreeIndexConfig(starTree2Dimensions, starTree2Metrics)));
    addTableConfig(starTreeTableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, defaultTableConfig, schema, 0, defaultTableSegmentDir, defaultTableTarDir);
    uploadSegments(DEFAULT_TABLE_NAME, defaultTableTarDir);
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, starTreeTableConfig, schema, 0, starTreeTableSegmentDir, starTreeTableTarDir);
    uploadSegments(STAR_TREE_TABLE_NAME, starTreeTableTarDir);

    // Set up the query generators
    SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(defaultTableTarDir.getPath());
    List<String> aggregationFunctions = new ArrayList<>(AGGREGATION_FUNCTION_TYPES.size());
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      aggregationFunctions.add(functionType.getName());
    }
    _starTree1QueryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, starTree1Dimensions, starTree1Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions);
    _starTree2QueryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, starTree2Dimensions, starTree2Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions);

    // Wait for all documents loaded
    _currentTable = DEFAULT_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
    _currentTable = STAR_TREE_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
  }

  private static StarTreeIndexConfig getStarTreeIndexConfig(List<String> dimensions, List<String> metrics) {
    List<String> functionColumnPairs = new ArrayList<>();
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      for (String metric : metrics) {
        functionColumnPairs.add(new AggregationFunctionColumnPair(functionType, metric).toColumnName());
      }
    }
    return new StarTreeIndexConfig(dimensions, null, functionColumnPairs, 100);
  }

  @Test
  public void testGeneratedQueries()
      throws Exception {
    for (int i = 0; i < NUM_QUERIES_TO_GENERATE; i += 2) {
      testStarQuery(_starTree1QueryGenerator.nextQuery());
      testStarQuery(_starTree2QueryGenerator.nextQuery());
    }
  }

  @Test
  public void testPredicateOnMetrics()
      throws Exception {
    String starQuery;

    // Query containing predicate on one metric only
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0";
    testStarQuery(starQuery);
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay BETWEEN 0 and 10000";
    testStarQuery(starQuery);

    // Query containing predicate on multiple metrics
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0 AND ArrDelay > 0";
    testStarQuery(starQuery);

    // Query containing predicate on multiple metrics and dimensions
    starQuery =
        "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0 AND ArrDelay > 0 AND OriginStateName = 'Massachusetts'";
    testStarQuery(starQuery);
  }

  private void testStarQuery(String starQuery)
      throws Exception {
    String referenceQuery = starQuery.replace(STAR_TREE_TABLE_NAME, DEFAULT_TABLE_NAME) + " TOP 10000";
    JsonNode starResponse = postQuery(starQuery);
    JsonNode referenceResponse = postQuery(referenceQuery);

    // Skip comparison if not all results returned in reference response
    if (referenceResponse.has("aggregationResults")) {
      JsonNode aggregationResults = referenceResponse.get("aggregationResults").get(0);
      if (aggregationResults.has("groupByResult") && aggregationResults.get("groupByResult").size() == 10000) {
        return;
      }
    }

    Assert.assertTrue(QueryComparison.compare(starResponse, referenceResponse, false),
        "Query comparison failed for: \nStar Query: " + starQuery + "\nStar Response: " + starResponse
            + "\nReference Query: " + referenceQuery + "\nReference Response: " + referenceResponse);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);
    dropOfflineTable(STAR_TREE_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
