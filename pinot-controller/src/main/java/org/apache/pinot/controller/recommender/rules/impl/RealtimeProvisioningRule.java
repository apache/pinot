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

package org.apache.pinot.controller.recommender.rules.impl;

import com.google.common.io.Files;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.realtime.provisioning.MemoryEstimator;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.configs.IndexConfig;
import org.apache.pinot.controller.recommender.rules.io.params.RealtimeProvisioningRuleParams;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * This rule gives some recommendations useful for provisioning real time tables. Specifically it provides some
 * recommendations for optimal segments size, total memory used per host, and consuming memory used per host based
 * on the provided characteristics of the data.
 */
public class RealtimeProvisioningRule extends AbstractRule {
  public static String OPTIMAL_SEGMENT_SIZE = "Optimal Segment Size";
  public static String CONSUMING_MEMORY_PER_HOST = "Consuming Memory per Host";
  public static String TOTAL_MEMORY_USED_PER_HOST = "Total Memory Used per Host";

  private final RealtimeProvisioningRuleParams _params;

  public RealtimeProvisioningRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getRealtimeProvisioningRuleParams();
  }

  @Override
  public void run()
      throws InvalidInputException {

    if (_params == null) {
      // no realtime provisioning params provided; skip
      return;
    }

    // prepare input to memory estimator
    TableConfig tableConfig =
        createTableConfig(_output.getIndexConfig(), _input.getSchema(), _output.isAggregateMetrics());
    long maxUsableHostMemoryByte = DataSizeUtils.toBytes(_params.getMaxUsableHostMemory());
    int totalConsumingPartitions = _params.getNumPartitions() * _params.getNumReplicas();
    int ingestionRatePerPartition = (int) _input.getNumMessagesPerSecInKafkaTopic() / _params.getNumPartitions();
    int retentionHours = _params.getRealtimeTableRetentionHours();
    int[] numHosts = _params.getNumHosts();
    int[] numHours = _params.getNumHours();

    File workingDir = Files.createTempDir();
    MemoryEstimator memoryEstimator =
        new MemoryEstimator(tableConfig,
            _input.getSchema(),
            _input.getSchemaWithMetadata(),
            _params.getNumRowsInGeneratedSegment(),
            ingestionRatePerPartition,
            maxUsableHostMemoryByte,
            retentionHours,
            workingDir);
    try {
      // run memory estimator
      File statsFile = memoryEstimator.initializeStatsHistory();
      memoryEstimator.estimateMemoryUsed(statsFile, numHosts, numHours, totalConsumingPartitions, retentionHours);

      // extract recommendations
      extractResults(memoryEstimator, numHosts, numHours, _output.getRealtimeProvisioningRecommendations());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private TableConfig createTableConfig(IndexConfig indexConfig, Schema schema, boolean aggregateMetrics) {
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME);
    tableConfigBuilder.setTableName(schema.getSchemaName());
    tableConfigBuilder.setLoadMode("MMAP");

    // indices
    setIfNotEmpty(indexConfig.getSortedColumn(), tableConfigBuilder::setSortedColumn);
    setIfNotEmpty(indexConfig.getBloomFilterColumns(), tableConfigBuilder::setBloomFilterColumns);
    setIfNotEmpty(indexConfig.getNoDictionaryColumns(), tableConfigBuilder::setNoDictionaryColumns);
    setIfNotEmpty(indexConfig.getInvertedIndexColumns(), tableConfigBuilder::setInvertedIndexColumns);
    setIfNotEmpty(indexConfig.getOnHeapDictionaryColumns(), tableConfigBuilder::setOnHeapDictionaryColumns);
    setIfNotEmpty(indexConfig.getVariedLengthDictionaryColumns(), tableConfigBuilder::setVarLengthDictionaryColumns);

    TableConfig tableConfig = tableConfigBuilder.build();
    tableConfig.getIndexingConfig().setAggregateMetrics(aggregateMetrics);
    return tableConfig;
  }

  private void setIfNotEmpty(String colName, Consumer<String> func) {
    if (colName != null && !colName.isEmpty()) {
      func.accept(colName);
    }
  }

  private void setIfNotEmpty(Set<String> colNames, Consumer<List<String>> func) {
    if (colNames != null && !colNames.isEmpty()) {
      func.accept(new ArrayList<>(colNames));
    }
  }

  private void extractResults(MemoryEstimator memoryEstimator, int[] numHosts, int[] numHours,
      Map<String, Map<String, String>> rtProvRecommendations) {
    Map<String, String> segmentSizes = makeMatrix(memoryEstimator.getOptimalSegmentSize(), numHosts, numHours);
    Map<String, String> consumingMemory = makeMatrix(memoryEstimator.getConsumingMemoryPerHost(), numHosts, numHours);
    Map<String, String> totalMemory = makeMatrix(memoryEstimator.getActiveMemoryPerHost(), numHosts, numHours,
        element -> element.substring(0, element.indexOf('/'))); // take the first number (eg: 48G/48G)
    rtProvRecommendations.put(OPTIMAL_SEGMENT_SIZE, segmentSizes);
    rtProvRecommendations.put(CONSUMING_MEMORY_PER_HOST, consumingMemory);
    rtProvRecommendations.put(TOTAL_MEMORY_USED_PER_HOST, totalMemory);
  }

  private Map<String, String> makeMatrix(String[][] elements, int[] numHosts, int[] numHours) {
    return makeMatrix(elements, numHosts, numHours, Function.identity());
  }

  /**
   * This functions creates a matrix (of type map) for the elements. For example:
   * {
   *   "numHosts -   ": "2         4         6         ",
   *   "numHours -  2": "2.55G     1.36G     869.19M   ",
   *   "numHours -  3": "3.74G     2G        1.25G     ",
   *   "numHours -  4": "4.94G     2.63G     1.65G     ",
   *   "numHours -  5": "6.14G     3.27G     2.05G     "
   * }
   */
  private Map<String, String> makeMatrix(String[][] elements, int[] numHosts, int[] numHours,
      Function<String, String> elementTrimmingFunc) {
    Map<String, String> output = new LinkedHashMap<>();

    // numHosts line
    String cellFormat = "%-10s";
    String numHostsValues =
        Arrays.stream(numHosts).mapToObj(n -> String.format(cellFormat, n)).collect(Collectors.joining());
    output.put("numHosts -   ", numHostsValues);

    // numHours lines
    for (int i = 0; i < elements.length; i++) {
      String[] rowElements = elements[i];
      String rowKey = String.format("numHours - %2d", numHours[i]);
      String rowValues = Arrays.stream(rowElements).map(elementTrimmingFunc).map(e -> String.format(cellFormat, e))
          .collect(Collectors.joining());
      output.put(rowKey, rowValues);
    }

    return output;
  }
}
