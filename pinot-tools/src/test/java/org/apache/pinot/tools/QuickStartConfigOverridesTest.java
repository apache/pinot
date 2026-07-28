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
package org.apache.pinot.tools;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Regression tests for quickstart configuration handling:
///
/// - `-configFile` entries must be honored by quickstarts overriding `getConfigOverrides()`
///   (they were silently dropped before the base implementation started reading the file).
///   On key collisions the quickstart's hardcoded values win — the convention every
///   quickstart follows since the quickstart consolidation (#19090).
/// - Multiple `-bootstrapTableDir` arguments must each be bootstrapped (the option declares
///   arity `1..*`).
public class QuickStartConfigOverridesTest {

  @Test
  public void testConfigFileOverridesAreMergedInQuickstart()
      throws Exception {
    Path configFile = Files.createTempFile("quickstart-overrides-test", ".properties");
    try {
      Files.writeString(configFile, "pinot.broker.stats.enabled=true\n"
          + CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT + "=false\n");
      Quickstart quickStart = new Quickstart();
      quickStart.setConfigFilePath(configFile.toString());

      Map<String, Object> overrides = quickStart.getConfigOverrides();
      assertEquals(String.valueOf(overrides.get("pinot.broker.stats.enabled")), "true",
          "config-file entries must be present in the overrides");
      assertEquals(overrides.get(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT), true,
          "hardcoded quickstart defaults win over config-file values on collision");
    } finally {
      Files.deleteIfExists(configFile);
    }
  }

  @Test
  public void testHardcodedDefaultsPresentWithoutConfigFile() {
    Map<String, Object> overrides = new Quickstart().getConfigOverrides();
    assertEquals(overrides.get(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT), true);
  }

  @Test
  public void testMultipleBootstrapTableDirsAreAllBootstrapped()
      throws Exception {
    File tmpRoot = Files.createTempDirectory("quickstart-bootstrap-test").toFile();
    try {
      // Two minimal table dirs: <name>/<name>_schema.json + <name>_offline_table_config.json +
      // rawdata/<name>_data.csv (the layout copyFilesystemTableToTmpDirectory validates).
      for (String table : new String[] {"tableA", "tableB"}) {
        File tableDir = new File(tmpRoot, table);
        File rawData = new File(tableDir, "rawdata");
        assertTrue(rawData.mkdirs());
        Files.writeString(new File(tableDir, table + "_schema.json").toPath(), "{}");
        Files.writeString(new File(tableDir, table + "_offline_table_config.json").toPath(), "{}");
        Files.writeString(new File(rawData, table + "_data.csv").toPath(), "col\n1\n");
      }
      File quickstartTmp = new File(tmpRoot, "quickstartTmp");
      assertTrue(quickstartTmp.mkdirs());

      Quickstart quickStart = new Quickstart();
      quickStart.setBootstrapDataDirs(new String[] {
          new File(tmpRoot, "tableA").getAbsolutePath(),
          new File(tmpRoot, "tableB").getAbsolutePath()
      });

      List<QuickstartTableRequest> requests = quickStart.bootstrapOfflineTableDirectories(quickstartTmp);
      assertEquals(requests.size(), 2, "every -bootstrapTableDir entry must be bootstrapped");
      assertTrue(new File(quickstartTmp, "tableA").isDirectory());
      assertTrue(new File(quickstartTmp, "tableB").isDirectory());
    } finally {
      FileUtils.deleteQuietly(tmpRoot);
    }
  }
}
