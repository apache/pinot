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
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class MinionTaskUtilsTest {

  @Test
  public void testGetInputPinotFS() throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("input.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getInputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testGetOutputPinotFS() throws Exception {
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
}
