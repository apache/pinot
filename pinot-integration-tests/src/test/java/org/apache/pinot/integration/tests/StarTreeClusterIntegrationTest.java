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
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.integration.tests.startree.SegmentInfoProvider;
import org.apache.pinot.integration.tests.startree.StarTreeQueryGenerator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


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
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_single_value_columns.schema";
  private static final int NUM_STAR_TREE_DIMENSIONS = 5;
  private static final int NUM_STAR_TREE_METRICS = 5;
  private static final List<AggregationFunctionType> AGGREGATION_FUNCTION_TYPES =
      Arrays.asList(AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE,
          AggregationFunctionType.DISTINCTCOUNTBITMAP);
  private static final int NUM_QUERIES_TO_GENERATE = 100;

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);

  private StarTreeQueryGenerator _starTree1QueryGenerator;
  private StarTreeQueryGenerator _starTree2QueryGenerator;

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
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);

    // Pick fixed dimensions and metrics for the first star-tree
    List<String> starTree1Dimensions =
        Arrays.asList("OriginCityName", "DepTimeBlk", "LongestAddGTime", "CRSDepTime", "DivArrDelay");
    List<String> starTree1Metrics =
        Arrays.asList("CarrierDelay", "DepDelay", "LateAircraftDelay", "ArrivalDelayGroups", "ArrDel15");
    int starTree1MaxLeafRecords = 10;

    // Randomly pick some dimensions and metrics for the second star-tree
    List<String> allDimensions = new ArrayList<>(schema.getDimensionNames());
    Collections.shuffle(allDimensions, _random);
    List<String> starTree2Dimensions = allDimensions.subList(0, NUM_STAR_TREE_DIMENSIONS);
    List<String> allMetrics = new ArrayList<>(schema.getMetricNames());
    Collections.shuffle(allMetrics, _random);
    List<String> starTree2Metrics = allMetrics.subList(0, NUM_STAR_TREE_METRICS);
    int starTree2MaxLeafRecords = 100;

    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(
        Arrays.asList(getStarTreeIndexConfig(starTree1Dimensions, starTree1Metrics, starTree1MaxLeafRecords),
            getStarTreeIndexConfig(starTree2Dimensions, starTree2Metrics, starTree2MaxLeafRecords)));
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Set up the query generators
    SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(_tarDir.getPath());
    List<String> aggregationFunctions = new ArrayList<>(AGGREGATION_FUNCTION_TYPES.size());
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      aggregationFunctions.add(functionType.getName());
    }
    _starTree1QueryGenerator = new StarTreeQueryGenerator(DEFAULT_TABLE_NAME, starTree1Dimensions, starTree1Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions, _random);
    _starTree2QueryGenerator = new StarTreeQueryGenerator(DEFAULT_TABLE_NAME, starTree2Dimensions, starTree2Metrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions, _random);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  private static StarTreeIndexConfig getStarTreeIndexConfig(List<String> dimensions, List<String> metrics,
      int maxLeafRecords) {
    List<String> functionColumnPairs = new ArrayList<>();
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      for (String metric : metrics) {
        functionColumnPairs.add(new AggregationFunctionColumnPair(functionType, metric).toColumnName());
      }
    }
    return new StarTreeIndexConfig(dimensions, null, functionColumnPairs, null, maxLeafRecords);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int i = 0; i < NUM_QUERIES_TO_GENERATE; i += 2) {
      testStarQuery(_starTree1QueryGenerator.nextQuery(), !useMultiStageQueryEngine);
      testStarQuery(_starTree2QueryGenerator.nextQuery(), !useMultiStageQueryEngine);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardCodedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // This query can test the case of one predicate matches all the child nodes but star-node cannot be used because
    // the predicate is included as remaining predicate from another branch
    String starQuery = "SELECT DepTimeBlk, COUNT(*) FROM mytable "
        + "WHERE CRSDepTime BETWEEN 1137 AND 1849 AND DivArrDelay > 218 AND CRSDepTime NOT IN (35, 1633, 1457, 140) "
        + "AND LongestAddGTime NOT IN (17, 105, 20, 22) GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardCodedFilteredAggQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String starQuery = "SELECT DepTimeBlk, COUNT(*), COUNT(*) FILTER (WHERE CRSDepTime = 35) FROM mytable "
        + "WHERE CRSDepTime != 35"
        + "GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    // Don't verify that the query plan uses StarTree index, as this query results in FILTER_EMPTY in the query plan.
    // This is still a valuable test, as it caught a bug where only the subFilterContext was being preserved through
    // AggragationFunctionUtils#buildFilteredAggregateProjectOperators
    testStarQuery(starQuery, false);

    // Ensure the filtered agg and unfiltered agg can co-exist in one query
    starQuery = "SELECT DepTimeBlk, COUNT(*), COUNT(*) FILTER (WHERE DivArrDelay > 20) FROM mytable "
        + "WHERE CRSDepTime != 35"
        + "GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT DepTimeBlk, COUNT(*) FILTER (WHERE CRSDepTime != 35) FROM mytable "
        + "GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);
  }

  private void testStarQuery(String starQuery)
      throws Exception {
    testStarQuery(starQuery, true);
  }

  private void testStarQuery(String starQuery, boolean verifyPlan)
      throws Exception {
    String filterStartreeIndex = "FILTER_STARTREE_INDEX";
    String explain = "EXPLAIN PLAN FOR ";
    String disableStarTree = "SET useStarTree = false; ";

    if (verifyPlan) {
      JsonNode starPlan = postQuery(explain + starQuery);
      JsonNode referencePlan = postQuery(disableStarTree + explain + starQuery);
      assertTrue(starPlan.toString().contains(filterStartreeIndex)
              || starPlan.toString().contains("FILTER_EMPTY")
              || starPlan.toString().contains("ALL_SEGMENTS_PRUNED_ON_SERVER"),
          "StarTree query did not indicate use of StarTree index in query plan. Plan: " + starPlan);
      assertFalse("Reference query indicated use of StarTree index in query plan. Plan: " + referencePlan,
          referencePlan.toString().contains(filterStartreeIndex));
    }

    JsonNode starResponse = postQuery(starQuery);
    String referenceQuery = disableStarTree + starQuery;
    JsonNode referenceResponse = postQuery(referenceQuery);
    assertEquals(starResponse.get("resultTable"), referenceResponse.get("resultTable"), String.format(
        "Query comparison failed for: \n"
            + "Star Query: %s\nStar Response: %s\nReference Query: %s\nReference Response: %s\nRandom Seed: %d",
        starQuery, starResponse, referenceQuery, referenceResponse, _randomSeed));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
