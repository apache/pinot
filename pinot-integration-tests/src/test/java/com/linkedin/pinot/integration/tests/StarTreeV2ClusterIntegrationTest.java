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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Extends the integration test for star-tree V1 with the following changes:
 * <ul>
 *   <li>Only pick a subset of dimensions and metrics to generate star-tree</li>
 *   <li>Test against more aggregation function types (COUNT, MIN, MAX, SUM, AVG, MIN_MAX_RANGE)</li>
 *   <li>
 *     DISTINCT_COUNT_HLL, PERCENTILE_EST, PERCENTILE_TDIGEST are not included because the results are estimated values
 *   </li>
 * </ul>
 */
public class StarTreeV2ClusterIntegrationTest extends StarTreeClusterIntegrationTest {
  private static final int NUM_STAR_TREE_DIMENSIONS = 10;
  private static final int NUM_STAR_TREE_METRICS = 5;
  private static final List<AggregationFunctionType> AGGREGATION_FUNCTION_TYPES =
      Arrays.asList(AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.SUM, AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE);

  private List<String> _starTreeDimensions;
  private List<String> _starTreeMetrics;

  @Override
  protected void setUpSegmentsAndQueryGenerator() throws Exception {
    // Randomly pick some dimensions and metrics for star-tree V2
    List<String> allDimensions = new ArrayList<>(_schema.getDimensionNames());
    Collections.shuffle(allDimensions);
    _starTreeDimensions = new ArrayList<>(NUM_STAR_TREE_DIMENSIONS);
    for (int i = 0; i < NUM_STAR_TREE_DIMENSIONS; i++) {
      _starTreeDimensions.add(allDimensions.get(i));
    }
    List<String> allMetrics = new ArrayList<>(_schema.getMetricNames());
    Collections.shuffle(allMetrics);
    _starTreeMetrics = new ArrayList<>(NUM_STAR_TREE_METRICS);
    for (int i = 0; i < NUM_STAR_TREE_METRICS; i++) {
      _starTreeMetrics.add(allMetrics.get(i));
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
    _queryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, _starTreeDimensions, _starTreeMetrics,
        segmentInfoProvider.getSingleValueDimensionValuesMap(), aggregationFunctions);

    // Create and upload segments with star tree indexes from Avro data
    createAndUploadSegments(avroFiles, STAR_TREE_TABLE_NAME, true);
  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName, boolean createStarTreeIndex)
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    List<StarTreeV2BuilderConfig> starTreeV2BuilderConfigs = null;
    if (createStarTreeIndex) {
      Set<AggregationFunctionColumnPair> functionColumnPairs = new HashSet<>();
      for (AggregationFunctionType functionType : AGGREGATION_FUNCTION_TYPES) {
        for (String metric : _starTreeMetrics) {
          functionColumnPairs.add(new AggregationFunctionColumnPair(functionType, metric));
        }
      }
      StarTreeV2BuilderConfig starTreeV2BuilderConfig =
          new StarTreeV2BuilderConfig.Builder().setDimensionsSplitOrder(_starTreeDimensions)
              .setFunctionColumnPairs(functionColumnPairs)
              .build();
      starTreeV2BuilderConfigs = Collections.singletonList(starTreeV2BuilderConfig);
    }

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName, false,
        starTreeV2BuilderConfigs, null, _schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(_tarDir);
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
