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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class StarTreeFunctionParametersIntegrationTest extends BaseClusterIntegrationTest {
  private TableConfig _tableConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    // Set max segment startree preprocess parallelism to 6
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(8));
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNumReplicas(getNumReplicas()).build();
    _tableConfig.getIndexingConfig().setStarTreeIndexConfigs(new ArrayList<>());
    _tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    addTableConfig(_tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, _tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    // Stop the Pinot cluster
    stopServer();
    stopBroker();
    stopController();

    // Stop Zookeeper
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  // Use the smallest sample dataset so that dynamic creation and deletion of star-tree indexes during segment reload
  // can be tested in a reasonable time on busy CI environments.
  @Override
  protected String getAvroTarFileName() {
    return "On_Time_On_Time_Performance_2014_Min_100_subset_nonulls.tar.gz";
  }

  @Override
  protected long getCountStarResult() {
    return 100;
  }

  @Test
  public void testStarTreeWithDistinctCountHllConfigurations() throws Exception {
    // Get results of DISTINCTCOUNTHLL with log2m = 4,6,8 before building any star-tree indexes to compare results
    // later and ensure that the star-tree indexes are being built correctly.
    int distinctCountHllLog2m4 = getDistinctCountResult("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 4) FROM mytable "
        + "WHERE DistanceGroup > 1");
    int distinctCountHllLog2m6 = getDistinctCountResult("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 6) FROM mytable "
        + "WHERE DistanceGroup > 1");
    int distinctCountHllLog2m8 = getDistinctCountResult("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 8) FROM mytable "
        + "WHERE DistanceGroup > 1");

    // Ensure that the results are different
    assert distinctCountHllLog2m4 != distinctCountHllLog2m6;
    assert distinctCountHllLog2m6 != distinctCountHllLog2m8;
    assert distinctCountHllLog2m4 != distinctCountHllLog2m8;

    List<StarTreeIndexConfig> starTreeIndexConfigs = _tableConfig.getIndexingConfig().getStarTreeIndexConfigs();
    StarTreeAggregationConfig aggregationConfig = new StarTreeAggregationConfig("OriginAirportSeqID",
        "DISTINCTCOUNTHLL", Map.of(Constants.HLL_LOG2M_KEY, 4), null, null, null, null, null);

    starTreeIndexConfigs.add(new StarTreeIndexConfig(Collections.singletonList("DistanceGroup"), null,
        null, List.of(aggregationConfig), 1));
    updateTableConfig(_tableConfig);
    waitForTableConfigUpdate(tableConfig -> tableConfig.getIndexingConfig().getStarTreeIndexConfigs().size() == 1);
    reloadOfflineTable(DEFAULT_TABLE_NAME);

    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 4) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m4);
    checkQueryDoesNotUseStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m8);
    checkQueryDoesNotUseStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 8) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m8);

    // Remove the star-tree index for DISTINCTCOUNTHLL with log2m = 4 and add new star-tree index with default log2m

    aggregationConfig = new StarTreeAggregationConfig("OriginAirportSeqID", "DISTINCTCOUNTHLL",
        null, null, null, null, null, null);
    starTreeIndexConfigs.remove(starTreeIndexConfigs.size() - 1);
    starTreeIndexConfigs.add(new StarTreeIndexConfig(Collections.singletonList("DistanceGroup"), null,
        null, List.of(aggregationConfig), 1));

    updateTableConfig(_tableConfig);
    waitForTableConfigUpdate(tableConfig -> tableConfig.getIndexingConfig().getStarTreeIndexConfigs()
        .get(starTreeIndexConfigs.size() - 1).getAggregationConfigs().get(0).getFunctionParameters() == null);
    reloadOfflineTable(DEFAULT_TABLE_NAME);

    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m8);
    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 8) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m8);
    checkQueryDoesNotUseStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 4) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m4);

    // Add the star-tree index for DISTINCTCOUNTHLL with log2m = 4 again to ensure that two star-tree indexes can
    // be created for the same function / column pair with different configurations.

    aggregationConfig = new StarTreeAggregationConfig("OriginAirportSeqID", "DISTINCTCOUNTHLL",
        Map.of(Constants.HLL_LOG2M_KEY, "4"), null, null, null, null, null);
    starTreeIndexConfigs.add(new StarTreeIndexConfig(Collections.singletonList("DistanceGroup"), null,
        null, List.of(aggregationConfig), 1));
    updateTableConfig(_tableConfig);
    waitForTableConfigUpdate(tableConfig -> tableConfig.getIndexingConfig().getStarTreeIndexConfigs().size() == 2);
    reloadOfflineTable(DEFAULT_TABLE_NAME);

    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 4) FROM mytable WHERE DistanceGroup > 1",
        distinctCountHllLog2m4);
    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 8) FROM mytable WHERE DistanceGroup > 1",
        distinctCountHllLog2m8);

    // Update the previously added star-tree index to ensure that the star-tree index is rebuilt with new config
    // parameters
    aggregationConfig = new StarTreeAggregationConfig("OriginAirportSeqID", "DISTINCTCOUNTHLL",
        Map.of(Constants.HLL_LOG2M_KEY, "6"), null, null, null, null, null);
    starTreeIndexConfigs.remove(starTreeIndexConfigs.size() - 1);
    starTreeIndexConfigs.add(new StarTreeIndexConfig(Collections.singletonList("DistanceGroup"), null,
        null, List.of(aggregationConfig), 1));
    updateTableConfig(_tableConfig);
    waitForTableConfigUpdate(tableConfig -> tableConfig.getIndexingConfig().getStarTreeIndexConfigs()
        .get(starTreeIndexConfigs.size() - 1).getAggregationConfigs().get(0).getFunctionParameters()
        .get(Constants.HLL_LOG2M_KEY).equals("6"));
    reloadOfflineTable(DEFAULT_TABLE_NAME);

    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 6) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m6);
    // Ensure that the previous star-tree index was removed
    checkQueryDoesNotUseStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID, 4) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m4);
    // Check that the other star-tree isn't affected
    checkQueryUsesStarTreeIndex("SELECT DISTINCTCOUNTHLL(OriginAirportSeqID) FROM mytable "
            + "WHERE DistanceGroup > 1", distinctCountHllLog2m8);
  }

  /**
   * Helper method to wait for a table config to be updated. The provided condition is used to check if the table config
   * has been updated.
   */
  private void waitForTableConfigUpdate(Function<TableConfig, Boolean> condition) {
    // Wait for table config to be updated
    TestUtils.waitForCondition(
        (aVoid) -> {
          TableConfig tableConfig = getOfflineTableConfig();
          return condition.apply(tableConfig);
        }, 5_000L, "Failed to update table config"
    );
  }

  /**
   * Helper method to assert that a query does not use a star-tree index.
   */
  private void checkQueryDoesNotUseStarTreeIndex(String query, int expectedResult) throws Exception {
    JsonNode explainPlan = postQuery("EXPLAIN PLAN FOR " + query);
    assertFalse(explainPlan.toString().contains(StarTreeClusterIntegrationTest.FILTER_STARTREE_INDEX));
    assertEquals(getDistinctCountResult(query), expectedResult);
  }

  /**
   * Helper method to assert that a query uses a star-tree index. Waits if necessary for the star-tree index to be
   * built.
   */
  private void checkQueryUsesStarTreeIndex(String query, int expectedResult) throws Exception {
    // Wait for the star-tree indexes to be built if needed
    TestUtils.waitForCondition(
        (aVoid) -> {
          JsonNode result;
          try {
            result = postQuery("EXPLAIN PLAN FOR " + query);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return result.toString().contains(StarTreeClusterIntegrationTest.FILTER_STARTREE_INDEX);
        }, 1000L, 10_000L, "Failed to use star-tree index for query: " + query
    );
    assertEquals(getDistinctCountResult(query), expectedResult);
  }

  /**
   * Helper method to return the int result of a distinct count query.
   */
  private int getDistinctCountResult(String query) throws Exception {
    JsonNode result = postQuery(query);
    return result.get("resultTable").get("rows").get(0).get(0).asInt();
  }
}
