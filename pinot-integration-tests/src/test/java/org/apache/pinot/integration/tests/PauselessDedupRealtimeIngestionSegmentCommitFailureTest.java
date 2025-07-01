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
import java.util.List;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;

import static org.testng.Assert.assertNotNull;


public class PauselessDedupRealtimeIngestionSegmentCommitFailureTest
    extends PauselessRealtimeIngestionSegmentCommitFailureTest {

  private static final int NUM_PARTITIONS = 2;

  @Override
  protected String getAvroTarFileName() {
    return "dedupPauselessIngestionTestData.tar.gz";
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Override
  protected String getSchemaFileName() {
    return "dedupIngestionTestSchema.schema";
  }

  @Override
  protected long getCountStarResult() {
    // Two distinct records are expected with pk values of 0, 1.
    return 2;
  }

  @Override
  protected String getPartitionColumn() {
    return "id";
  }

  @Override
  protected int getNumReplicas() {
    return 2;
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

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(2);

    // load data in kafka
    List<File> avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(avroFiles);

    setMaxSegmentCompletionTimeMillis();
    // create schema for non-pauseless table
    Schema schema = createSchema();
    schema.setSchemaName(getNonPauselessTableName());
    addSchema(schema);

    // add non-pauseless table
    TableConfig tableConfig2 = createDedupTableConfig(avroFiles.get(0));
    tableConfig2.setTableName(TableNameBuilder.REALTIME.tableNameWithType(getNonPauselessTableName()));
    tableConfig2.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig2.getValidationConfig().setRetentionTimeValue("100000");
    addTableConfig(tableConfig2);
    waitForDocsLoaded(600_000L, true, tableConfig2.getTableName());

    // create schema for pauseless table
    schema.setSchemaName(getPauselessTableName());
    addSchema(schema);

    // add pauseless table
    TableConfig tableConfig = createDedupTableConfig(avroFiles.get(0));
    tableConfig.setTableName(TableNameBuilder.REALTIME.tableNameWithType(getPauselessTableName()));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    assertNotNull(tableConfig.getIngestionConfig());
    StreamIngestionConfig streamIngestionConfig = tableConfig.getIngestionConfig().getStreamIngestionConfig();
    assertNotNull(streamIngestionConfig);
    streamIngestionConfig.getStreamConfigMaps()
        .get(0)
        .put(StreamConfigProperties.PAUSELESS_SEGMENT_DOWNLOAD_TIMEOUT_SECONDS, "10");
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    streamIngestionConfig.setDisasterRecoveryMode(DisasterRecoveryMode.ALWAYS);
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);

    addTableConfig(tableConfig);
    String realtimeTableName = tableConfig.getTableName();
    TestUtils.waitForCondition(aVoid -> getNumErrorSegmentsInEV(realtimeTableName) > 0, 600_000L,
        "Segments still not in error state");
  }

  protected TableConfig createDedupTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createDedupTableConfig(sampleAvroFile, getPartitionColumn(), NUM_PARTITIONS);
    assertNotNull(tableConfig.getDedupConfig());
    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
    }
    return tableConfig;
  }
}
