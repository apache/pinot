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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.UpsertDedupTestFixtures.DEDUP_SURVIVORS;
import static org.apache.pinot.integration.tests.UpsertDedupTestFixtures.dedupRowsQuery;


public class BaseDedupIntegrationTest extends BaseClusterIntegrationTestSet {
  protected static final String DEDUP_TABLE_WITH_REPLICAS = "DedupTableWithReplicas";

  protected List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    //Test the cases for RF>1
    startServers(2);

    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    createDedupTable();
    createDedupTableWithReplicas();
  }

  protected void createDedupTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createDedupTableConfig(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfig);
    waitForAllDocsLoaded(600_000L);
    // Dedup keeps the first row per id, so the count is right before the duplicates are read: wait for them too
    waitForAllStreamRecordsConsumed(getTableName(), 600_000L);
  }

  protected void createDedupTableWithReplicas()
      throws IOException {
    Schema schemaWithReplicas = createSchema();
    schemaWithReplicas.setSchemaName(DEDUP_TABLE_WITH_REPLICAS);
    addSchema(schemaWithReplicas);
    TableConfig tableConfigWithReplicas =
        createDedupTableWithReplicas(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfigWithReplicas);
    BrokerRequest countStarBrokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT COUNT(*) FROM " + TableNameBuilder.REALTIME.tableNameWithType(DEDUP_TABLE_WITH_REPLICAS));
    long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(aVoid -> {
      // Ensure both servers are ready to be queried
      Integer serverPort0 = getServingServerPort(countStarBrokerRequest, 0L);
      Integer serverPort1 = getServingServerPort(countStarBrokerRequest, 1L);
      if (serverPort0 == null || serverPort1 == null || serverPort0.equals(serverPort1)) {
        return false;
      }
      // Query 10 times to get result from both servers (query should be distributed to both servers in a round-robin
      // fashion, but query more times to be more robust)
      for (int i = 0; i < 10; i++) {
        if (getCurrentCountStarResult(DEDUP_TABLE_WITH_REPLICAS) != countStarResult) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to load " + countStarResult + " documents to both servers");
    waitForAllStreamRecordsConsumed(DEDUP_TABLE_WITH_REPLICAS, 600_000L);
  }

  @Nullable
  private Integer getServingServerPort(BrokerRequest brokerRequest, long requestId) {
    BrokerRoutingManager routingManager = _brokerStarters.get(0).getRoutingManager();
    RoutingTable routingTable = routingManager.getRoutingTable(brokerRequest, requestId);
    if (routingTable == null) {
      return null;
    }
    Map<ServerInstance, SegmentsToQuery> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
    if (serverInstanceToSegmentsMap.size() != 1) {
      return null;
    }
    return serverInstanceToSegmentsMap.keySet().iterator().next().getPort();
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(getStreamConfigs())));
    assert ingestionConfig.getStreamIngestionConfig() != null;
    ingestionConfig.getStreamIngestionConfig()
        .setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY);
    ingestionConfig.getStreamIngestionConfig().setEnforceConsumptionInOrder(true);
    return ingestionConfig;
  }

  /// Creates a new Dedup enabled table config with replication=2 and metadatTTL=30
  protected TableConfig createDedupTableWithReplicas(File sampleAvroFile, String primaryKeyColumn,
      int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setMetadataTTL(30);
    dedupConfig.setPreload(Enablement.ENABLE);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(DEDUP_TABLE_WITH_REPLICAS)
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(2)
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setDedupConfig(dedupConfig)
        .build();
    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
    }
    return tableConfig;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // Create > 1 segments
    return 2;
  }

  @Override
  protected String getSchemaFileName() {
    return "dedupIngestionTestSchema.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "dedupIngestionTestData.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return "id";
  }

  @Override
  protected long getCountStarResult() {
    // 5 distinct records are expected with pk values of 0, 1, 2, 3, 4
    return 5;
  }

  //Tests the query results for table with RF=1
  @Test
  public void testValues()
      throws Exception {
    assertRows(dedupRowsQuery(getTableName()), DEDUP_SURVIVORS);
  }

  @Test
  public void testSegmentReload()
      throws Exception {
    String reloadJobId = reloadTableAndValidateResponse(getTableName(), TableType.REALTIME, false);
    // The supplier variant retries a failing status call, logs it and attaches the last error to a timeout
    TestUtils.waitForCondition(() -> isReloadJobCompleted(reloadJobId), 1000L, 600_000L,
        "Reload job " + reloadJobId + " did not finish", Duration.ofSeconds(30));

    // Push data again. Dedup drops every re-pushed row, so wait until the table has consumed them before checking.
    pushAvroIntoKafka(_avroFiles);
    waitForAllStreamRecordsConsumed(getTableName(), 600_000L);
    waitForClusterStateSettled(TableNameBuilder.REALTIME.tableNameWithType(getTableName()), 600_000L);
    assertRows(dedupRowsQuery(getTableName()), DEDUP_SURVIVORS);

    // The replicated table consumes the same topic, so it must drop the re-pushed rows on every replica too. A
    // subclass that does not create it (see createDedupTableWithReplicas()) skips this check.
    String replicatedTableName = TableNameBuilder.REALTIME.tableNameWithType(DEDUP_TABLE_WITH_REPLICAS);
    if (getHelixResourceManager().hasTable(replicatedTableName)) {
      waitForAllStreamRecordsConsumed(DEDUP_TABLE_WITH_REPLICAS, 600_000L);
      waitForClusterStateSettled(replicatedTableName, 600_000L);
      assertRowsFromEveryReplica(dedupRowsQuery(DEDUP_TABLE_WITH_REPLICAS), DEDUP_SURVIVORS);
    }
  }

  //tests the query results when metadataTTL is set with RF>1.
  @Test
  public void testValuesWithReplicas()
      throws Exception {
    // Both replica groups answer, as the setup checks with getServingServerPort()
    assertRowsFromEveryReplica(dedupRowsQuery(DEDUP_TABLE_WITH_REPLICAS), DEDUP_SURVIVORS);
  }
}
