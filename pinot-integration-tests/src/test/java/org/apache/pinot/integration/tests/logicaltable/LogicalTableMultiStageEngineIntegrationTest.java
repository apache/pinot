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
package org.apache.pinot.integration.tests.logicaltable;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.MultiStageEngineIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;


public class LogicalTableMultiStageEngineIntegrationTest extends MultiStageEngineIntegrationTest {

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();

    // Set the multi-stage max server query threads for the cluster, so that we can test the query queueing logic
    // in the MultiStageBrokerRequestHandler
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "30");

    startBroker();
    startServer(); // TODO - Do we want 2 servers similar to BaseLogicalTableIntegrationTest ?
    setupTenants();

    List<File> avroFiles = unpackAvroData(_tempDir);
    List<String> physicalTableNames = getOfflineTableNames();
    Map<String, List<File>> offlineTableDataFiles = distributeFilesToTables(physicalTableNames, avroFiles);
    for (Map.Entry<String, List<File>> entry : offlineTableDataFiles.entrySet()) {
      String tableName = entry.getKey();
      List<File> avroFilesForTable = entry.getValue();

      File tarDir = new File(_tarDir, tableName);

      TestUtils.ensureDirectoriesExistAndEmpty(tarDir);

      // Create and upload the schema and table config
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(tableName);
      addSchema(schema);
      TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
      addTableConfig(offlineTableConfig);

      // Create and upload segments
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFilesForTable, offlineTableConfig, schema, 0, _segmentDir,
          tarDir);
      uploadSegments(tableName, tarDir);
    }

    createLogicalTableAndSchema();

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
//    setupTableWithNonDefaultDatabase(avroFiles); TODO - Fix this
  }

  private List<String> getOfflineTableNames() {
    return List.of("physicalTable_0", "physicalTable_1");
  }

  @Override
  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected String getLogicalTableName() {
    return getTableName();
  }

  @Override
  protected LogicalTableConfig createLogicalTableConfig() {
    List<String> offlineTableNames = getOfflineTableNames().stream().map(TableNameBuilder.OFFLINE::tableNameWithType)
        .collect(Collectors.toList());
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (String physicalTableName : offlineTableNames) {
      physicalTableConfigMap.put(physicalTableName, new PhysicalTableConfig());
    }
    LogicalTableConfigBuilder builder =
        new LogicalTableConfigBuilder().setTableName(getTableName())
            .setBrokerTenant(getBrokerTenant())
            .setRefOfflineTableName(offlineTableNames.get(0))
            .setRefRealtimeTableName(null)
            .setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
  }

}
