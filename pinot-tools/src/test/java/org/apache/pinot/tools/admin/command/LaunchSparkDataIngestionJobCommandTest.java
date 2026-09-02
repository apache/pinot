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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.tools.admin.command.LaunchSparkDataIngestionJobCommand.SparkType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// accepts newline-separated entries so the format can change without breaking the Spark submit.
public class LaunchSparkDataIngestionJobCommandTest {
  private File _tempDir;
  private LocalPinotFS _fs;
  private LaunchSparkDataIngestionJobCommand _command;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("plugin-index-test").toFile();
    _fs = new LocalPinotFS();
    _command = new LaunchSparkDataIngestionJobCommand();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(_tempDir);
  }

  private String writeIndex(String name, String content)
      throws Exception {
    File f = new File(_tempDir, name);
    Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }

  @Test
  public void testReadsColonSeparatedIndexAsWrittenByBuildClasspath()
      throws Exception {
    String idx = writeIndex("colon.classpath",
        "plugin-libs/jackson-core-2.22.2.jar:plugin-libs/jackson-databind-2.22.2.jar");
    assertEquals(_command.readPluginClasspathIndex(_fs, idx),
        List.of("plugin-libs/jackson-core-2.22.2.jar", "plugin-libs/jackson-databind-2.22.2.jar"));
  }

  @Test
  public void testReadsNewlineSeparatedIndex()
      throws Exception {
    String idx = writeIndex("newline.classpath",
        "plugin-libs/a-1.jar\nplugin-libs/b-2.jar\n");
    assertEquals(_command.readPluginClasspathIndex(_fs, idx), List.of("plugin-libs/a-1.jar", "plugin-libs/b-2.jar"));
  }

  @Test
  public void testSkipsBlankAndWhitespaceEntries()
      throws Exception {
    String idx = writeIndex("blanks.classpath", "  plugin-libs/a-1.jar  ::\n\n plugin-libs/b-2.jar \n");
    assertEquals(_command.readPluginClasspathIndex(_fs, idx), List.of("plugin-libs/a-1.jar", "plugin-libs/b-2.jar"));
  }

  @Test
  public void testEmptyIndexYieldsNoDependencies()
      throws Exception {
    assertTrue(_command.readPluginClasspathIndex(_fs, writeIndex("empty.classpath", "")).isEmpty());
  }

  /// A plugin that ships no index at all - an old-format plugin keeping its jars beside it - must
  /// not fail the submit; its own jars have already been added by the directory walk.
  @Test
  public void testMissingIndexIsToleratedAndYieldsNoDependencies() {
    String missing = new File(_tempDir, "does-not-exist.classpath").getAbsolutePath();
    assertTrue(_command.readPluginClasspathIndex(_fs, missing).isEmpty());


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
}
