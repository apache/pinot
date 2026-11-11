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
import java.util.Map;
import java.util.Random;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.DISASTER_RECOVERY_MODE_CONFIG_KEY;
import static org.testng.Assert.assertNotNull;


/// Runs the shared server-failure recovery scenarios with deduplication and two replicas.
public class PauselessDedupRealtimeIngestionSegmentCommitFailureTest
    extends PauselessRealtimeIngestionSegmentCommitFailureTest {
  private static final int NUM_PARTITIONS = 2;

  private final double _randomDouble = new Random().nextDouble();

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

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    super.overrideControllerConf(properties);
    if (_randomDouble > 0.5) {
      properties.put(DISASTER_RECOVERY_MODE_CONFIG_KEY, "ALWAYS");
    }
  }

  @Override
  protected int getNumServersForTest() {
    return 2;
  }

  @Override
  protected TableConfig createTestTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createDedupTableConfig(sampleAvroFile, getPartitionColumn(), NUM_PARTITIONS);
    assertNotNull(tableConfig.getDedupConfig());
    return tableConfig;
  }

  @Override
  protected void configurePauselessTable(TableConfig tableConfig) {
    assertNotNull(tableConfig.getIngestionConfig());
    StreamIngestionConfig streamIngestionConfig = tableConfig.getIngestionConfig().getStreamIngestionConfig();
    assertNotNull(streamIngestionConfig);
    streamIngestionConfig.getStreamConfigMaps()
        .get(0)
        .put(StreamConfigProperties.PAUSELESS_SEGMENT_DOWNLOAD_TIMEOUT_SECONDS, "10");
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    if (_randomDouble <= 0.5) {
      streamIngestionConfig.setDisasterRecoveryMode(DisasterRecoveryMode.ALWAYS);
    }
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
  }

  @Override
  protected boolean hasExpectedErrorSegments(String realtimeTableName, int expectedMaxFailures) {
    return getNumErrorSegmentsInEV(realtimeTableName) > 0;
  }
}
