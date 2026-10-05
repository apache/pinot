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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import org.apache.pinot.tools.admin.command.LaunchSparkDataIngestionJobCommand.SparkType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class LaunchSparkDataIngestionJobCommandTest {

  /// `shouldLoadPlugin` skips any plugin directory whose name contains "spark", so that only the
  /// ingestion plugin for the selected Spark version is re-included, by comparing the directory
  /// name against [SparkType#getPluginName()]. The name must therefore be the directory the plugin
  /// actually ships in, which is its module name. It read "pinot-batch-ingestion-spark-3.2" while
  /// the module is pinot-batch-ingestion-spark-3, so nothing ever matched and a Spark ingestion job
  /// failed in the executor with `ClassNotFoundException: SparkSegmentGenerationJobRunner` unless
  /// the plugin was named explicitly with `-pluginsToLoad`.
  @Test
  public void testSparkPluginNameMatchesAnExistingBatchIngestionModule() {
    File batchIngestion = findBatchIngestionDir();
    assertNotNull(batchIngestion, "could not locate pinot-plugins/pinot-batch-ingestion");
    for (SparkType sparkType : SparkType.values()) {
      File module = new File(batchIngestion, sparkType.getPluginName());
      assertTrue(module.isDirectory(),
          sparkType + " declares plugin name '" + sparkType.getPluginName() + "', which is not a module under "
              + batchIngestion + ". shouldLoadPlugin() matches this against the plugin's directory name, so it must "
              + "be the module name.");
    }
  }

  /// Walks up from the working directory so the test does not care whether it is run from the
  /// module or from the repository root.
  private static File findBatchIngestionDir() {
    for (File dir = new File("").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
      File candidate = new File(dir, "pinot-plugins/pinot-batch-ingestion");
      if (candidate.isDirectory()) {
        return candidate;
      }
    }
    return null;
  }
}
