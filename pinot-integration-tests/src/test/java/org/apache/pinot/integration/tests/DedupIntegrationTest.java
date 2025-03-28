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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DedupIntegrationTest extends BaseClusterIntegrationTestSet {

  private List<File> _avroFiles;
  private static final String DEDUP_TABLE_WITH_REPLICAS = "DedupTableWithReplicas_REALTIME";
  private static final String DEDUP_WITH_REPLICAS_SCHEMA = "DedupTableWithReplicas";

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

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createDedupTableConfig(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfig);
    waitForAllDocsLoaded(600_000L);
    createDedupConfigsWithReplicas();
  }

  public void createDedupConfigsWithReplicas()
      throws IOException {
    Schema schemaWithReplicas = createSchema(getSchemaFileName());
    schemaWithReplicas.setSchemaName(DEDUP_WITH_REPLICAS_SCHEMA);
    addSchema(schemaWithReplicas);
    TableConfig tableConfigWithReplication =
        createDedupTableWithReplicas(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfigWithReplication);
    waitForDocsLoaded(600_000L, true, DEDUP_TABLE_WITH_REPLICAS);
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(getStreamConfigs())));
    ingestionConfig.getStreamIngestionConfig()
        .setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY);
    ingestionConfig.getStreamIngestionConfig().setEnforceConsumptionInOrder(true);
    return ingestionConfig;
  }

  /**
   * Creates a new Dedup enabled table config with replication=2 and metadatTTL=30
   */
  protected TableConfig createDedupTableWithReplicas(File sampleAvroFile, String primaryKeyColumn,
      int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    DedupConfig dedupConfig =
        new DedupConfig(true, HashFunction.NONE, null, new HashMap<>(), 30, getTimeColumnName(), true);
    return new TableConfigBuilder(TableType.REALTIME).setTableName("DedupTableWithReplicas_REALTIME")
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
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 2))
        .setDedupConfig(dedupConfig)
        .build();
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
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 5;
  }

  //Tests the query results for table with RF=1
  @Test
  public void testValues()
      throws Exception {
    assertEquals(getCurrentCountStarResult(), getCountStarResult());

    // Validate the older value persist
    for (int i = 0; i < getCountStarResult(); i++) {
      Assert.assertEquals(getPinotConnection()
          .execute("SELECT name FROM " + getTableName() + " WHERE id = " + i)
          .getResultSet(0)
          .getString(0),
          "" + i);
    }
  }

  @Test
  public void testSegmentReload()
      throws Exception {
    ControllerTest.sendPostRequest(
        StringUtil.join("/", getControllerBaseApiUrl(), "segments", getTableName(),
            "reload?forceDownload=false"), null);

    // wait for reload to finish
    Thread.sleep(1000);

    // Push data again
    pushAvroIntoKafka(_avroFiles);

    // Validate no change
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
    for (int i = 0; i < getCountStarResult(); i++) {
      Assert.assertEquals(getPinotConnection()
              .execute("SELECT name FROM " + getTableName() + " WHERE id = " + i)
              .getResultSet(0)
              .getString(0),
          "" + i);
    }
  }

  //tests the query results when metadataTTL is set with RF>1.
  @Test
  public void testValuesWithReplicas() {
    assertEquals(getCurrentCountStarResult(DEDUP_TABLE_WITH_REPLICAS), getCountStarResult());

    // Validate the older value persist
    for (int i = 0; i < getCountStarResult(); i++) {
      Assert.assertEquals(
          getPinotConnection().execute("SELECT name FROM " + DEDUP_TABLE_WITH_REPLICAS + " WHERE id = " + i)
              .getResultSet(0).getString(0), "" + i);
    }
  }
}
