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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
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
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE);
  private static final int NUM_QUERIES_TO_GENERATE = 100;

  private List<String> _starTree1Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
  private List<String> _starTree2Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
  private List<String> _starTree1Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
  private List<String> _starTree2Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
  private StarTreeQueryGenerator _starTree1QueryGenerator;
  private StarTreeQueryGenerator _starTree2QueryGenerator;

  private Schema _schema;
  private String _currentTable;

  @Nonnull
  @Override
  protected String getTableName() {
    return _currentTable;
  }

  @Nonnull
  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create the tables
    addOfflineTable(DEFAULT_TABLE_NAME);
    addOfflineTable(STAR_TREE_TABLE_NAME);

    // Set up segments and query generator
    _schema = Schema.fromFile(getSchemaFile());
    setUpSegmentsAndQueryGenerator();

    // Wait for all documents loaded
    _currentTable = DEFAULT_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
    _currentTable = STAR_TREE_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
  }

  private void setUpSegmentsAndQueryGenerator()
      throws Exception {
    // Randomly pick some dimensions and metrics for star-tree V2
    List<String> allDimensions = new ArrayList<>(_schema.getDimensionNames());
    Collections.shuffle(allDimensions);
    for (int i = 0; i < NUM_STAR_TREE_DIMENSIONS; i++) {
      _starTree1Dimensions.add(allDimensions.get(2 * i));
      _starTree2Dimensions.add(allDimensions.get(2 * i + 1));
    }
    List<String> allMetrics = new ArrayList<>(_schema.getMetricNames());
    Collections.shuffle(allMetrics);
    for (int i = 0; i < NUM_STAR_TREE_METRICS; i++) {
      _starTree1Metrics.add(allMetrics.get(2 * i));
      _starTree2Metrics.add(allMetrics.get(2 * i + 1));
    }

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments without star tree indexes from Avro data
    createAndUploadSegments(avroFiles, DEFAULT_TABLE_NAME, false);

    // Initialize the query generator using segments without star tree indexes
    SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(_tarDir.getAbsolutePath());
    List<String> aggregationFunctions = new ArrayList<>(AGGREGATION_FUNCTION_TYPES.size());
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      aggregationFunctions.add(functionType.getName());
    }
    _starTree1QueryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, _starTree1Dimensions, _starTree1Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions);
    _starTree2QueryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, _starTree2Dimensions, _starTree2Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions);

    // Create and upload segments with star tree indexes from Avro data
    createAndUploadSegments(avroFiles, STAR_TREE_TABLE_NAME, true);
  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName, boolean createStarTreeIndex)
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    List<StarTreeV2BuilderConfig> starTreeV2BuilderConfigs = null;
    if (createStarTreeIndex) {
      starTreeV2BuilderConfigs = Arrays.asList(getBuilderConfig(_starTree1Dimensions, _starTree1Metrics),
          getBuilderConfig(_starTree2Dimensions, _starTree2Metrics));
    }

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName, getTimeColumnName(),
            starTreeV2BuilderConfigs, null, _schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(getTableName(), _tarDir);
  }

  private static StarTreeV2BuilderConfig getBuilderConfig(List<String> dimensions, List<String> metrics) {
    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      for (String metric : metrics) {
        functionColumnPairs.add(new AggregationFunctionColumnPair(functionType, metric));
      }
    }
    return new StarTreeV2BuilderConfig.Builder().setDimensionsSplitOrder(dimensions)
        .setFunctionColumnPairs(functionColumnPairs).setMaxLeafRecords(10).build();
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
