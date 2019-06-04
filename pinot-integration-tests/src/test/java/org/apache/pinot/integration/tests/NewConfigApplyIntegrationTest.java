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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.CombinedConfig;
import org.apache.pinot.common.config.CombinedConfigLoader;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for the new config format command line tools.
 */
public class NewConfigApplyIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(NewConfigApplyIntegrationTest.class);

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start an empty cluster
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @Test(enabled = false)
  public void testTableOperations()
      throws Exception {
    // Unpack the table configs if necessary
    List<String> configs =
        Lists.newArrayList("mytable.conf", "mytable-updated.conf", "profiles/test1.conf", "profiles/test2.conf");
    List<File> configFiles = configs.stream().map(path -> getClass().getClassLoader().getResource(path))
        .map(TestUtils::getFileFromResourceUrl).map(File::new).collect(Collectors.toList());

    // Apply expects the profiles to be relative to cwd, so copy them
    FileUtils.copyDirectoryToDirectory(configFiles.get(2).getParentFile(), new File("."));

    // Create a new table using the command line tools without a profile
    runAdminCommand("ApplyTableConfig", "-controllerUrl", _controllerBaseApiUrl, "-tableConfigFile",
        configFiles.get(0).getAbsolutePath());

    // Check that the table exists
    String tableConfiguration = sendGetRequestRaw(_controllerBaseApiUrl + "/v2/tables/mytable");
    CombinedConfig tableConfigurationObject = CombinedConfigLoader.loadCombinedConfig(tableConfiguration);
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getTableName(), "mytable_OFFLINE");
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getValidationConfig().getReplicationNumber(), 3);

    // Update the table using a configuration profile
    runAdminCommand("ApplyTableConfig", "-controllerUrl", _controllerBaseApiUrl, "-tableConfigFile",
        configFiles.get(1).getAbsolutePath(), "-profile", "test2");

    // Check that the table is updated
    tableConfiguration = sendGetRequestRaw(_controllerBaseApiUrl + "/v2/tables/mytable");
    tableConfigurationObject = CombinedConfigLoader.loadCombinedConfig(tableConfiguration);
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getTableName(), "mytable_OFFLINE");
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getValidationConfig().getReplicationNumber(), 4);
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getIndexingConfig().getLoadMode(), "MMAP");

    // Update the table using a configuration profile
    runAdminCommand("ApplyTableConfig", "-controllerUrl", _controllerBaseApiUrl, "-tableConfigFile",
        configFiles.get(0).getAbsolutePath(), "-profile", "test1");

    // Check that the table is updated
    tableConfiguration = sendGetRequestRaw(_controllerBaseApiUrl + "/v2/tables/mytable");
    tableConfigurationObject = CombinedConfigLoader.loadCombinedConfig(tableConfiguration);
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getTableName(), "mytable_OFFLINE");
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getValidationConfig().getReplicationNumber(), 3);
    assertEquals(tableConfigurationObject.getOfflineTableConfig().getIndexingConfig().getLoadMode(), "HEAP");
  }

  private void runAdminCommand(String... args)
      throws Exception {
    ArrayList<String> commandLine = Lists
        .newArrayList("java", "-cp", "pinot-tools/target/pinot-tool-launcher-jar-with-dependencies.jar",
            "org.apache.pinot.tools.admin.PinotAdministrator");

    commandLine.addAll(Lists.newArrayList(args));

    LOGGER.info("Running command " + Joiner.on(" ").join(commandLine));

    Process process =
        new ProcessBuilder(commandLine.toArray(new String[0])).redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT).start();
    int returnCode = process.waitFor();
    assertEquals(returnCode, 0);
  }
}
