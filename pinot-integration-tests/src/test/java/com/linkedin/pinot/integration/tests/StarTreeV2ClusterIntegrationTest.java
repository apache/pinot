/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import com.linkedin.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import com.linkedin.pinot.tools.query.comparison.SegmentInfoProvider;
import com.linkedin.pinot.tools.query.comparison.StarTreeQueryGenerator;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Extends the integration test for star-tree V1 with the following changes:
 * <ul>
 *   <li>Only pick a subset of dimensions and metrics, and generate 2 different star-trees</li>
 *   <li>Test against more aggregation function types (COUNT, MIN, MAX, SUM, AVG, MIN_MAX_RANGE)</li>
 *   <li>
 *     DISTINCT_COUNT_HLL, PERCENTILE_EST, PERCENTILE_TDIGEST are not included because the results are estimated values
 *   </li>
 * </ul>
 */
public class StarTreeV2ClusterIntegrationTest extends StarTreeClusterIntegrationTest {
  private static final Random RANDOM = new Random();

  private static final int NUM_STAR_TREE_DIMENSIONS = 5;
  private static final int NUM_STAR_TREE_METRICS = 5;
  private static final List<AggregationFunctionType> AGGREGATION_FUNCTION_TYPES =
      Arrays.asList(AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE);

  private List<String> _starTree1Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
  private List<String> _starTree2Dimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
  private List<String> _starTree1Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
  private List<String> _starTree2Metrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
  private StarTreeQueryGenerator _starTree1QueryGenerator;
  private StarTreeQueryGenerator _starTree2QueryGenerator;

  @Override
  protected void setUpSegmentsAndQueryGenerator() throws Exception {
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
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName, false,
        starTreeV2BuilderConfigs, null, _schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(_tarDir);
  }

  private static StarTreeV2BuilderConfig getBuilderConfig(List<String> dimensions, List<String> metrics) {
    Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
    for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
      for (String metric : metrics) {
        functionColumnPairs.add(new AggregationFunctionColumnPair(functionType, metric));
      }
    }
    return new StarTreeV2BuilderConfig.Builder().setDimensionsSplitOrder(dimensions)
        .setFunctionColumnPairs(functionColumnPairs)
        .setMaxLeafRecords(10)
        .build();
  }

  @Override
  protected String generateQuery() {
    if (RANDOM.nextBoolean()) {
      return _starTree1QueryGenerator.nextQuery();
    } else {
      return _starTree2QueryGenerator.nextQuery();
    }
  }

  @Test(enabled = false)
  @Override
  public void testQueriesFromQueryFile() {
    // Ignored
  }

  @Test(enabled = false)
  @Override
  public void testPredicateOnMetrics() {
    // Ignored
  }
}
