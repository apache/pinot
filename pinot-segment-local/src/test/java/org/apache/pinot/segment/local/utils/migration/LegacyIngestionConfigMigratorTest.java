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
package org.apache.pinot.segment.local.utils.migration;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.ConfigMigrationUtils;
import org.apache.pinot.spi.config.migration.MigrationResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LegacyIngestionConfigMigratorTest {

  private static Map<String, String> streamConfigs() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, "kafka");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME), "topic");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        "cf");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_DECODER_CLASS), "dec");
    return streamConfigMap;
  }

  @Test
  public void testDefaultRegistryUpgradesLegacyTableConfig() {
    Map<String, String> streamConfigs = streamConfigs();
    TableConfig legacy = new TableConfigBuilder(TableType.REALTIME).setTableName("legacyTable")
        .setSegmentPushType("APPEND")
        .setSegmentPushFrequency("HOURLY")
        .setStreamConfigs(streamConfigs)
        .build();
    assertNull(legacy.getIngestionConfig());
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(legacy), 0);

    ConfigMigrationRegistry registry = DefaultConfigMigrationRegistry.create();
    assertEquals(registry.getCurrentTableConfigVersion(), 1);

    MigrationResult<TableConfig> result = registry.migrateTableConfig(legacy);
    assertTrue(result.isChanged());
    assertEquals(result.getVersion(), 1);

    TableConfig migrated = result.getConfig();
    // Deprecated ingestion fields are folded into ingestionConfig.
    IngestionConfig ingestionConfig = migrated.getIngestionConfig();
    assertNotNull(ingestionConfig);
    assertNotNull(ingestionConfig.getBatchIngestionConfig());
    assertEquals(ingestionConfig.getBatchIngestionConfig().getSegmentIngestionType(), "APPEND");
    assertNotNull(ingestionConfig.getStreamIngestionConfig());
    assertEquals(ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps().get(0), streamConfigs);
    // Deprecated fields are cleared and the version marker is stamped.
    assertNull(migrated.getIndexingConfig().getStreamConfigs());
    assertNull(migrated.getValidationConfig().getSegmentPushType());
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(migrated), 1);
  }

  @Test
  public void testUpgradesDeprecatedStreamConfigsOnly() {
    // A REALTIME table whose only deprecated field is tableIndexConfig.streamConfigs.
    Map<String, String> streamConfigs = streamConfigs();
    TableConfig legacy = new TableConfigBuilder(TableType.REALTIME).setTableName("streamOnly")
        .setStreamConfigs(streamConfigs)
        .build();

    MigrationResult<TableConfig> result = DefaultConfigMigrationRegistry.create().migrateTableConfig(legacy);
    assertTrue(result.isChanged());
    TableConfig migrated = result.getConfig();
    assertNotNull(migrated.getIngestionConfig().getStreamIngestionConfig());
    assertEquals(migrated.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0), streamConfigs);
    assertNull(migrated.getIndexingConfig().getStreamConfigs());
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(migrated), 1);
  }

  @Test
  public void testUpgradesDeprecatedBatchPushConfigsOnly() {
    // An OFFLINE table whose only deprecated fields are the segmentsConfig batch push settings.
    TableConfig legacy = new TableConfigBuilder(TableType.OFFLINE).setTableName("batchOnly")
        .setSegmentPushType("REFRESH")
        .setSegmentPushFrequency("DAILY")
        .build();

    MigrationResult<TableConfig> result = DefaultConfigMigrationRegistry.create().migrateTableConfig(legacy);
    assertTrue(result.isChanged());
    TableConfig migrated = result.getConfig();
    BatchIngestionConfig batchIngestionConfig = migrated.getIngestionConfig().getBatchIngestionConfig();
    assertNotNull(batchIngestionConfig);
    assertEquals(batchIngestionConfig.getSegmentIngestionType(), "REFRESH");
    assertEquals(batchIngestionConfig.getSegmentIngestionFrequency(), "DAILY");
    assertNull(migrated.getValidationConfig().getSegmentPushType());
    assertNull(migrated.getValidationConfig().getSegmentPushFrequency());
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(migrated), 1);
  }

  @Test
  public void testUpgradeSurvivesZkSerializationRoundTrip()
      throws Exception {
    // Simulate the real controller flow: an old version stored a deprecated config as a ZNRecord; the migration task
    // reads it back, migrates, and the upgraded config re-serializes cleanly with the version marker persisted.
    TableConfig legacy = new TableConfigBuilder(TableType.REALTIME).setTableName("roundTrip")
        .setSegmentPushType("APPEND")
        .setStreamConfigs(streamConfigs())
        .build();

    // Round-trip through the ZK ZNRecord form used by ZKMetadataProvider.
    TableConfig stored = TableConfigSerDeUtils.fromZNRecord(TableConfigSerDeUtils.toZNRecord(legacy));
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(stored), 0);

    MigrationResult<TableConfig> result = DefaultConfigMigrationRegistry.create().migrateTableConfig(stored);
    assertTrue(result.isChanged());

    // Re-serialize the migrated config (as the task persists it) and read it back: upgrade + marker both survive.
    TableConfig persisted = TableConfigSerDeUtils.fromZNRecord(TableConfigSerDeUtils.toZNRecord(result.getConfig()));
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(persisted), 1);
    assertNotNull(persisted.getIngestionConfig().getStreamIngestionConfig());
    assertNotNull(persisted.getIngestionConfig().getBatchIngestionConfig());
    assertNull(persisted.getIndexingConfig().getStreamConfigs());
    assertNull(persisted.getValidationConfig().getSegmentPushType());
  }

  @Test
  public void testAlreadyMigratedConfigIsNoOp() {
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("currentTable").build();
    ConfigMigrationUtils.setTableConfigVersion(config, 1);

    ConfigMigrationRegistry registry = DefaultConfigMigrationRegistry.create();
    MigrationResult<TableConfig> result = registry.migrateTableConfig(config);
    assertFalse(result.isChanged());
    assertEquals(result.getVersion(), 1);
  }
}
