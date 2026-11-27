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
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.pinot.tools.admin.command.LaunchSparkDataIngestionJobCommand.SparkType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers how a Spark ingestion job decides which jars to ship to the executors, and that it refuses a distribution
/// built with the thin-plugins profile.
public class LaunchSparkDataIngestionJobCommandTest {
  private File _tempDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("spark-deps-test").toFile();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  /// Builds a tree shaped like the default distribution: core in lib/, one fat jar per plugin under
  /// plugins/<type>/<name>/, and the external-process plugins under plugins-external/.
  private File buildDistribution()
      throws Exception {
    File dist = new File(_tempDir, "dist-" + System.nanoTime());
    for (String f : List.of("lib/pinot-all-1.0.jar",
        "plugins/pinot-input-format/pinot-json/pinot-json-1.0-shaded.jar",
        "plugins/pinot-stream-ingestion/pinot-kafka-4.0/pinot-kafka-4.0-1.0-shaded.jar",
        "plugins/pinot-file-system/pinot-s3/pinot-s3-1.0-shaded.jar",
        "plugins-external/pinot-batch-ingestion/pinot-batch-ingestion-spark-3/spark3-1.0-shaded.jar",
        "plugins-external/pinot-batch-ingestion/pinot-batch-ingestion-spark-2/spark2-1.0-shaded.jar",
        "plugins/pinot-connectors/pinot-spark-3-connector/spark-connector-1.0.jar")) {
      FileUtils.touch(new File(dist, f));
    }
    return dist;
  }

  /// picocli applies the option defaults when parsing a command line. A directly constructed instance has none, so
  /// the tests set the ones that the selection logic reads.
  private static LaunchSparkDataIngestionJobCommand commandWithCliDefaults() {
    LaunchSparkDataIngestionJobCommand cmd = new LaunchSparkDataIngestionJobCommand();
    cmd.setSparkVersion(SparkType.SPARK_3);
    cmd.setPluginsToExclude(List.of("pinot-kafka-4.0"));
    return cmd;
  }

  private static List<String> names(List<String> paths) {
    return paths.stream().map(FilenameUtils::getName).sorted().collect(Collectors.toList());
  }

  @Test
  public void testDefaultSelection()
      throws Exception {
    List<String> resolved = names(commandWithCliDefaults().resolveDepsJars(buildDistribution().getAbsolutePath()));
    assertTrue(resolved.contains("pinot-all-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("pinot-json-1.0-shaded.jar"), resolved.toString());
    assertTrue(resolved.contains("pinot-s3-1.0-shaded.jar"), resolved.toString());
    // The default excludes the Kafka plugin because its Scala dependencies break on a running Spark.
    assertFalse(resolved.contains("pinot-kafka-4.0-1.0-shaded.jar"), resolved.toString());
    // The ingestion plugin for the selected Spark version is the one exception to the rule that "spark"-named
    // plugin directories are skipped. Another Spark version and the Spark connector stay behind.
    assertTrue(resolved.contains("spark3-1.0-shaded.jar"), resolved.toString());
    assertFalse(resolved.contains("spark2-1.0-shaded.jar"), resolved.toString());
    assertFalse(resolved.contains("spark-connector-1.0.jar"), resolved.toString());
  }

  @Test
  public void testExplicitSelection()
      throws Exception {
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    cmd.setPluginsToLoad(List.of("pinot-s3"));
    List<String> resolved = names(cmd.resolveDepsJars(buildDistribution().getAbsolutePath()));
    assertTrue(resolved.contains("pinot-s3-1.0-shaded.jar"), resolved.toString());
    assertTrue(resolved.contains("spark3-1.0-shaded.jar"), resolved.toString());
    assertFalse(resolved.contains("pinot-json-1.0-shaded.jar"), resolved.toString());
  }

  /// The Spark ingestion jar does not link against the thin pinot-all. A thin distribution has a plugin-libs/
  /// store and a pinot-plugin.classpath index per plugin. Either one is enough to refuse it, as a path and as a URI.
  @Test
  public void testRefusesThinDistribution()
      throws Exception {
    File withStore = buildDistribution();
    FileUtils.touch(new File(withStore, "plugin-libs/jackson-core-2.22.jar"));
    File withIndex = buildDistribution();
    Files.writeString(new File(withIndex, "plugins/pinot-file-system/pinot-s3/pinot-plugin.classpath").toPath(),
        "plugin-libs/s3-2.54.jar", StandardCharsets.UTF_8);
    for (File thin : List.of(withStore, withIndex)) {
      for (String dir : List.of(thin.getAbsolutePath(), thin.toURI().toString())) {
        assertThrows(UnsupportedOperationException.class, () -> commandWithCliDefaults().resolveDepsJars(dir));
      }
    }
  }

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

  @Test
  public void testLegacyKafkaPluginSelection() {
    for (String pluginName : List.of("pinot-kafka-2.0", "pinot-kafka-3.0", "pinot-kafka-4.0")) {
      LaunchSparkDataIngestionJobCommand command = new LaunchSparkDataIngestionJobCommand();
      new CommandLine(command).parseArgs("-jobSpecFile", "unused", "-pluginsToExclude", pluginName);
      assertFalse(command.shouldLoadPlugin("pinot-kafka-4.0"));
      assertTrue(command.shouldLoadPlugin("pinot-parquet"));
      new CommandLine(command).parseArgs("-jobSpecFile", "unused", "-pluginsToLoad", pluginName);
      assertTrue(command.shouldLoadPlugin("pinot-kafka-4.0"));
      assertFalse(command.shouldLoadPlugin("pinot-parquet"));
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
