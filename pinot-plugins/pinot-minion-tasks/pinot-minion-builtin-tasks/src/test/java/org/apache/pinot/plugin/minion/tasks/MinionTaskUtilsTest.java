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
package org.apache.pinot.plugin.minion.tasks;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class MinionTaskUtilsTest {

  @Test
  public void testGetInputPinotFS()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("input.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getInputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testGetOutputPinotFS()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("output.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testIsLocalOutputDir() {
    assertTrue(MinionTaskUtils.isLocalOutputDir("file"));
    assertFalse(MinionTaskUtils.isLocalOutputDir("hdfs"));
  }

  @Test
  public void testGetLocalPinotFs() {
    assertTrue(MinionTaskUtils.getLocalPinotFs() instanceof LocalPinotFS);
  }

  @Test
  public void testNormalizeDirectoryURI() {
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir"));
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir/"));
  }

  @Test
  public void testExtractMinionAllowDownloadFromServer() {
    Map<String, String> configs = new HashMap<>();
    TableTaskConfig tableTaskConfig = new TableTaskConfig(
        Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();

    // Test when the configuration is not set, should return the default value which is false
    assertFalse(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));

    // Test when the configuration is set to true
    configs.put(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER, "true");
    tableTaskConfig = new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertTrue(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));

    // Test when the configuration is set to false
    configs.put(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER, "false");
    tableTaskConfig = new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertFalse(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));
  }

  @Test
  public void testGetValidDocIdsTypeDefaultBehavior() {
    // Test default behavior scenarios
    UpsertConfig upsertConfig = new UpsertConfig();
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: Default when delete is not enabled
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT);

    // Test 2: Default when delete is enabled
    upsertConfig.setDeleteRecordColumn("deleted");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeExplicitConfigurations() {
    // Test explicit configuration scenarios
    UpsertConfig upsertConfig = new UpsertConfig();
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: Explicit SNAPSHOT with enabled snapshot
    upsertConfig.setSnapshot(Enablement.ENABLE);
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT);

    // Test 2: Explicit SNAPSHOT with default snapshot enablement
    upsertConfig = new UpsertConfig(); // Reset to default
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT);

    // Test 3: Explicit IN_MEMORY without delete
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY");
    ValidDocIdsType result3 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result3, ValidDocIdsType.IN_MEMORY);

    // Test 4: Explicit IN_MEMORY_WITH_DELETE with delete column
    upsertConfig.setDeleteRecordColumn("deleted");
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    ValidDocIdsType result4 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result4, ValidDocIdsType.IN_MEMORY_WITH_DELETE);

    // Test 5: Explicit SNAPSHOT_WITH_DELETE with delete column
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    ValidDocIdsType result5 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result5, ValidDocIdsType.SNAPSHOT_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeBackwardCompatibilityAndOverride() {
    // Test backward compatibility behavior when delete is enabled
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setDeleteRecordColumn("deleted");
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: SNAPSHOT gets overridden to SNAPSHOT_WITH_DELETE
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 2: IN_MEMORY gets overridden to SNAPSHOT_WITH_DELETE
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 3: SNAPSHOT_WITH_DELETE stays the same
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    ValidDocIdsType result3 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result3, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 4: IN_MEMORY_WITH_DELETE stays the same (not overridden)
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    ValidDocIdsType result4 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result4, ValidDocIdsType.IN_MEMORY_WITH_DELETE);

    // Test 5: Case insensitive override behavior
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "in_memory_with_delete");
    ValidDocIdsType result5 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result5, ValidDocIdsType.IN_MEMORY_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeValidationErrors() {
    // Test validation error scenarios

    // Test 1: SNAPSHOT with disabled snapshot
    UpsertConfig upsertConfig1 = new UpsertConfig();
    upsertConfig1.setSnapshot(Enablement.DISABLE);
    Map<String, String> taskConfigs1 = new HashMap<>();
    taskConfigs1.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    IllegalStateException exception1 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT with snapshot disabled",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig1, taskConfigs1, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception1.getMessage(), "'snapshot' must not be 'DISABLE' with validDocIdsType: SNAPSHOT");

    // Test 2: IN_MEMORY_WITH_DELETE without delete column
    UpsertConfig upsertConfig2 = new UpsertConfig();
    Map<String, String> taskConfigs2 = new HashMap<>();
    taskConfigs2.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    IllegalStateException exception2 = expectThrows(
        "Expected IllegalStateException when using IN_MEMORY_WITH_DELETE without delete column",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig2, taskConfigs2, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception2.getMessage(),
        "'deleteRecordColumn' must be provided with validDocIdsType: IN_MEMORY_WITH_DELETE");

    // Test 3: SNAPSHOT_WITH_DELETE without delete column
    UpsertConfig upsertConfig3 = new UpsertConfig();
    Map<String, String> taskConfigs3 = new HashMap<>();
    taskConfigs3.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    IllegalStateException exception3 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT_WITH_DELETE without delete column",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig3, taskConfigs3, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception3.getMessage(),
        "'deleteRecordColumn' must be provided with validDocIdsType: SNAPSHOT_WITH_DELETE");

    // Test 4: SNAPSHOT_WITH_DELETE with disabled snapshot
    UpsertConfig upsertConfig4 = new UpsertConfig();
    upsertConfig4.setDeleteRecordColumn("deleted");
    upsertConfig4.setSnapshot(Enablement.DISABLE);
    Map<String, String> taskConfigs4 = new HashMap<>();
    taskConfigs4.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    IllegalStateException exception4 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT_WITH_DELETE with snapshot disabled",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig4, taskConfigs4, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception4.getMessage(),
        "'snapshot' must not be 'DISABLE' with validDocIdsType: SNAPSHOT_WITH_DELETE");
  }
}
