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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.pinot.integration.tests.startree.SegmentInfoProvider;
import org.apache.pinot.integration.tests.startree.StarTreeQueryGenerator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for Star-Tree based indexes.
 * <ul>
 *   <li>
 *     Set up the Pinot cluster and create a table with star tree indexes
 *   </li>
 *   <li>
 *     Send queries with and without star-tree and assert that results match
 *   </li>
 * </ul>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class StarTreeTest extends CustomDataQueryClusterIntegrationTest {
  public static final String FILTER_STARTREE_INDEX = "FILTER_STARTREE_INDEX";
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_columns.schema";
  private static final int NUM_STAR_TREE_DIMENSIONS = 5;
  private static final int NUM_STAR_TREE_METRICS = 6;
  private static final List<AggregationFunctionType> AGGREGATION_FUNCTION_TYPES =
      Arrays.asList(AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE,
          AggregationFunctionType.DISTINCTCOUNTBITMAP);
  private static final int NUM_QUERIES_TO_GENERATE = 100;

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);

  private StarTreeQueryGenerator _starTree1QueryGenerator;
  private StarTreeQueryGenerator _starTree2QueryGenerator;

  // Fixed dimensions and metrics for the first star-tree
  private final List<String> _starTree1Dimensions =
      Arrays.asList("OriginCityName", "DepTimeBlk", "LongestAddGTime", "CRSDepTime", "DivArrDelay");
  private final List<String> _starTree1Metrics =
      Arrays.asList("CarrierDelay", "DepDelay", "LateAircraftDelay", "ArrivalDelayGroups", "ArrDel15", "AirlineID");

  // Randomly picked dimensions and metrics for the second star-tree (initialized in setUp)
  private List<String> _starTree2Dimensions;
  private List<String> _starTree2Metrics;

  @Override
  public String getTableName() {
    return "StarTreeTest";
  }

  @Override
  public Schema createSchema() {
    try {
      InputStream inputStream = getClass().getClassLoader().getResourceAsStream(SCHEMA_FILE_NAME);
      Assert.assertNotNull(inputStream);
      Schema schema = Schema.fromInputStream(inputStream);
      schema.setSchemaName(getTableName());
      return schema;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return unpackAvroData(_tempDir);
  }

  // NOTE: Star-Tree and SegmentInfoProvider does not work on no-dictionary dimensions
  @Override
  protected List<String> getNoDictionaryColumns() {
    return Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay");
  }

  @Override
  public String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    Schema schema = createSchema();

    // Randomly pick some dimensions and metrics for the second star-tree
    // Exclude TotalAddGTime since it's a multi-value column, should not be in dimension split order.
    List<String> allDimensions = new ArrayList<>(schema.getDimensionNames());
    allDimensions.remove("TotalAddGTime");
    Collections.shuffle(allDimensions, _random);
    _starTree2Dimensions = allDimensions.subList(0, NUM_STAR_TREE_DIMENSIONS);
    List<String> allMetrics = new ArrayList<>(schema.getMetricNames());
    Collections.shuffle(allMetrics, _random);
    _starTree2Metrics = allMetrics.subList(0, NUM_STAR_TREE_METRICS);

    int starTree1MaxLeafRecords = 10;
    int starTree2MaxLeafRecords = 100;
    int starTree3MaxLeafRecords = 10;

    // Tests StarTree aggregate for multi-value column
    List<String> starTree3Dimensions =
        Arrays.asList("OriginCityName", "DepTimeBlk", "LongestAddGTime", "CRSDepTime", "DivArrDelay");

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .build();
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(
        Arrays.asList(
            getStarTreeIndexConfig(_starTree1Dimensions, _starTree1Metrics, starTree1MaxLeafRecords),
            getStarTreeIndexConfig(_starTree2Dimensions, _starTree2Metrics, starTree2MaxLeafRecords),
            getStarTreeIndexConfigForMVColAgg(starTree3Dimensions, starTree3MaxLeafRecords)));

    return tableConfig;
  }

  /**
   * Initialize query generators after segments have been built and uploaded.
   * This is called lazily before the first test that needs them.
   */
  private void ensureQueryGeneratorsInitialized()
      throws Exception {
    if (_starTree1QueryGenerator == null || _starTree2QueryGenerator == null) {
      List<String> aggregationFunctions = new ArrayList<>(AGGREGATION_FUNCTION_TYPES.size());
      for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
        aggregationFunctions.add(functionType.getName());
      }
      SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(_tarDir.getPath());
      _starTree1QueryGenerator = new StarTreeQueryGenerator(getTableName(), _starTree1Dimensions, _starTree1Metrics,
          segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions, _random);
      _starTree2QueryGenerator = new StarTreeQueryGenerator(getTableName(), _starTree2Dimensions, _starTree2Metrics,
          segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions, _random);
    }
  }

  private static StarTreeIndexConfig getStarTreeIndexConfig(List<String> dimensions, List<String> metrics,
      int maxLeafRecords) {
    List<StarTreeAggregationConfig> aggregationConfigs = new ArrayList<>();
    // Use default setting for COUNT(*) and custom setting for other aggregations for better coverage
    aggregationConfigs.add(new StarTreeAggregationConfig("*", "COUNT"));
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      if (functionType == AggregationFunctionType.COUNT) {
        continue;
      }
      for (String metric : metrics) {
        aggregationConfigs.add(
            new StarTreeAggregationConfig(metric, functionType.name(), null, CompressionCodec.LZ4, false, 4, null,
                null));
      }
    }
    return new StarTreeIndexConfig(dimensions, null, null, aggregationConfigs, maxLeafRecords);
  }

  private static StarTreeIndexConfig getStarTreeIndexConfigForMVColAgg(List<String> dimensions, int maxLeafRecords) {
    List<StarTreeAggregationConfig> aggregationConfigs = new ArrayList<>();
    aggregationConfigs.add(new StarTreeAggregationConfig("TotalAddGTime", "COUNTMV"));
    aggregationConfigs.add(new StarTreeAggregationConfig("TotalAddGTime", "SUMMV"));
    aggregationConfigs.add(new StarTreeAggregationConfig("TotalAddGTime", "AVGMV"));
    return new StarTreeIndexConfig(dimensions, null, null, aggregationConfigs, maxLeafRecords);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    ensureQueryGeneratorsInitialized();
    for (int i = 0; i < NUM_QUERIES_TO_GENERATE; i += 2) {
      testStarQuery(_starTree1QueryGenerator.nextQuery(), false);
      testStarQuery(_starTree2QueryGenerator.nextQuery(), false);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardCodedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();

    // This query can test the case of one predicate matches all the child nodes but star-node cannot be used because
    // the predicate is included as remaining predicate from another branch
    String starQuery = "SELECT DepTimeBlk, COUNT(*) FROM " + tableName
        + " WHERE CRSDepTime BETWEEN 1137 AND 1849 AND DivArrDelay > 218 AND CRSDepTime NOT IN (35, 1633, 1457, 140) "
        + "AND LongestAddGTime NOT IN (17, 105, 20, 22) GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    // Test MIN, MAX, SUM rewrite on LONG col
    starQuery = "SELECT MIN(AirlineID), MAX(AirlineID), SUM(AirlineID) FROM " + tableName
        + " WHERE CRSDepTime BETWEEN 1137 AND 1849";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SET enableNullHandling=true; SELECT COUNT(DivArrDelay) FROM " + tableName
        + " WHERE DivArrDelay > 218";
    testStarQuery(starQuery, false);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardCodedFilteredAggQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();

    String starQuery = "SELECT DepTimeBlk, COUNT(*), COUNT(*) FILTER (WHERE CRSDepTime = 35) FROM " + tableName
        + " WHERE CRSDepTime != 35 GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, false);

    // Ensure the filtered agg and unfiltered agg can co-exist in one query
    starQuery = "SELECT DepTimeBlk, COUNT(*), COUNT(*) FILTER (WHERE DivArrDelay > 20) FROM " + tableName
        + " WHERE CRSDepTime != 35 GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT DepTimeBlk, COUNT(*) FILTER (WHERE CRSDepTime != 35) FROM " + tableName
        + " GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMultiValueColumnAggregations(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();

    String starQuery = "SELECT COUNTMV(TotalAddGTime), SUMMV(TotalAddGTime), AVGMV(TotalAddGTime) FROM " + tableName;
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT OriginCityName, COUNTMV(TotalAddGTime), AVGMV(TotalAddGTime), SUMMV(TotalAddGTime) FROM "
        + tableName + " GROUP BY OriginCityName ORDER BY OriginCityName";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT DepTimeBlk, SUMMV(TotalAddGTime), AVGMV(TotalAddGTime) FROM " + tableName
        + " WHERE CRSDepTime > 1000 GROUP BY DepTimeBlk ORDER BY DepTimeBlk";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT OriginCityName, DepTimeBlk, SUMMV(TotalAddGTime) FROM " + tableName
        + " GROUP BY OriginCityName, DepTimeBlk ORDER BY OriginCityName, DepTimeBlk LIMIT 100";
    testStarQuery(starQuery, !useMultiStageQueryEngine);

    starQuery = "SELECT CRSDepTime, AVGMV(TotalAddGTime) FROM " + tableName
        + " WHERE CRSDepTime BETWEEN 800 AND 1200 AND DivArrDelay < 100 "
        + "GROUP BY CRSDepTime ORDER BY CRSDepTime";
    testStarQuery(starQuery, !useMultiStageQueryEngine);
  }

  private void testStarQuery(String starQuery, boolean verifyPlan)
      throws Exception {
    String explain = "EXPLAIN PLAN FOR ";
    String disableStarTree = "SET useStarTree = false; ";
    // The star-tree index doesn't currently support null values, but we should still be able to use the star-tree index
    // here since there aren't actually any null values in the dataset.
    String nullHandlingEnabled = "SET enableNullHandling = true; ";

    if (verifyPlan) {
      JsonNode starPlan = postQuery(explain + starQuery);
      JsonNode referencePlan = postQuery(disableStarTree + explain + starQuery);
      JsonNode nullHandlingEnabledPlan = postQuery(nullHandlingEnabled + explain + starQuery);
      assertTrue(starPlan.toString().contains(FILTER_STARTREE_INDEX) || starPlan.toString().contains("FILTER_EMPTY")
              || starPlan.toString().contains("ALL_SEGMENTS_PRUNED_ON_SERVER"),
          "StarTree query did not indicate use of star-tree index in query plan. Plan: " + starPlan);
      assertFalse(referencePlan.toString().contains(FILTER_STARTREE_INDEX),
          "Reference query indicated use of star-tree index in query plan. Plan: " + referencePlan);
      assertTrue(
          nullHandlingEnabledPlan.toString().contains(FILTER_STARTREE_INDEX) || nullHandlingEnabledPlan.toString()
              .contains("FILTER_EMPTY") || nullHandlingEnabledPlan.toString().contains("ALL_SEGMENTS_PRUNED_ON_SERVER"),
          "StarTree query with null handling enabled did not indicate use of star-tree index in query plan. Plan: "
              + nullHandlingEnabledPlan);
    }

    JsonNode starResponse = postQuery(starQuery);
    String referenceQuery = disableStarTree + starQuery;
    JsonNode referenceResponse = postQuery(referenceQuery);
    // Don't compare the actual response values since they could differ (e.g. "null" vs "Infinity" for MIN
    // aggregation function with no values aggregated)
    JsonNode nullHandlingEnabledResponse = postQuery(nullHandlingEnabled + starQuery);
    assertEquals(starResponse.get("exceptions").size(), 0);
    assertEquals(referenceResponse.get("exceptions").size(), 0);
    assertEquals(nullHandlingEnabledResponse.get("exceptions").size(), 0);
    assertEquals(starResponse.get("resultTable"), referenceResponse.get("resultTable"), String.format(
        "Query comparison failed for: \n"
            + "Star Query: %s\nStar Response: %s\nReference Query: %s\nReference Response: %s\nRandom Seed: %d",
        starQuery, starResponse, referenceQuery, referenceResponse, _randomSeed));
  }
}
