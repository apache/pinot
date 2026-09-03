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
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.tools.admin.command.LaunchSparkDataIngestionJobCommand.SparkType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Covers how a Spark ingestion job decides which jars to ship to the executors, and how it reads
/// the per-plugin shared-store dependency index. `dependency:build-classpath` writes that index as a
/// classpath fragment, so entries arrive ':'-separated on one line; the reader also accepts
/// newline-separated entries so the format can change without breaking the Spark submit.
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

  /// Builds a distribution-shaped tree: core in lib/, one thin jar per plugin under
  /// plugins/<type>/<name>/ with its shared-store index beside it, the shared store in
  /// plugin-libs/, and the external-process plugins under plugins-external/.
  private File buildDistribution()
      throws Exception {
    File dist = new File(_tempDir, "dist-" + System.nanoTime());
    for (String d : List.of("lib", "plugin-libs", "plugins/pinot-input-format/pinot-json",
        "plugins/pinot-stream-ingestion/pinot-kafka-3.0", "plugins/pinot-file-system/pinot-s3",
        "plugins-external/pinot-batch-ingestion/pinot-batch-ingestion-spark-3")) {
      FileUtils.forceMkdir(new File(dist, d));
    }
    for (String f : List.of("lib/pinot-all-1.0.jar",
        "plugin-libs/jackson-core-2.22.jar", "plugin-libs/kafka-clients-3.9.jar",
        "plugin-libs/scala-library-2.13.jar", "plugin-libs/s3-2.54.jar",
        "plugins/pinot-input-format/pinot-json/pinot-json-1.0.jar",
        "plugins/pinot-stream-ingestion/pinot-kafka-3.0/pinot-kafka-3.0-1.0.jar",
        "plugins/pinot-file-system/pinot-s3/pinot-s3-1.0.jar",
        "plugins-external/pinot-batch-ingestion/pinot-batch-ingestion-spark-3/spark3-1.0-shaded.jar")) {
      FileUtils.touch(new File(dist, f));
    }
    Files.write(new File(dist, "plugins/pinot-input-format/pinot-json/pinot-plugin.classpath").toPath(),
        "plugin-libs/jackson-core-2.22.jar".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(dist, "plugins/pinot-stream-ingestion/pinot-kafka-3.0/pinot-plugin.classpath").toPath(),
        "plugin-libs/kafka-clients-3.9.jar:plugin-libs/scala-library-2.13.jar".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(dist, "plugins/pinot-file-system/pinot-s3/pinot-plugin.classpath").toPath(),
        "plugin-libs/s3-2.54.jar:plugin-libs/jackson-core-2.22.jar".getBytes(StandardCharsets.UTF_8));
    return dist;
  }

  /// picocli applies the option defaults when parsing a command line; a directly constructed
  /// instance has none, so tests set the ones the selection logic reads.
  private static LaunchSparkDataIngestionJobCommand commandWithCliDefaults() {
    LaunchSparkDataIngestionJobCommand cmd = new LaunchSparkDataIngestionJobCommand();
    cmd.setSparkVersion(LaunchSparkDataIngestionJobCommand.SparkType.SPARK_3);
    cmd.setPluginsToExclude(List.of("pinot-kafka-3.0"));
    return cmd;
  }

  private static List<String> names(List<String> paths) {
    return paths.stream().map(FilenameUtils::getName).sorted().collect(Collectors.toList());
  }

  /// The default excludes the Kafka plugin because its Scala dependencies break on a running Spark.
  /// The shared store must not slip those in behind that exclusion.
  @Test
  public void testDefaultSelectionExcludesKafkaPluginAndItsSharedDependencies()
      throws Exception {
    File dist = buildDistribution();
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    List<String> resolved = names(cmd.resolveDepsJars(dist.getAbsolutePath()));

    assertTrue(resolved.contains("pinot-all-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("pinot-json-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("pinot-s3-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("jackson-core-2.22.jar"), resolved.toString());
    assertTrue(resolved.contains("s3-2.54.jar"), resolved.toString());
    // the excluded plugin, and the store jars only it declares
    assertFalse(resolved.contains("pinot-kafka-3.0-1.0.jar"), resolved.toString());
    assertFalse(resolved.contains("kafka-clients-3.9.jar"), resolved.toString());
    assertFalse(resolved.contains("scala-library-2.13.jar"), resolved.toString());
    // spark plugins run inside the executor and are never shipped
    assertFalse(resolved.contains("spark3-1.0-shaded.jar"), resolved.toString());
  }

  /// An explicit selection must ship the named plugin *with* its dependencies, which before the
  /// index meant shipping the plugin jar alone.
  @Test
  public void testExplicitSelectionShipsOnlyThatPluginAndItsDependencies()
      throws Exception {
    File dist = buildDistribution();
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    cmd.setPluginsToLoad(List.of("pinot-s3"));
    List<String> resolved = names(cmd.resolveDepsJars(dist.getAbsolutePath()));

    assertTrue(resolved.contains("pinot-s3-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("s3-2.54.jar"), resolved.toString());
    assertTrue(resolved.contains("jackson-core-2.22.jar"), resolved.toString());
    assertFalse(resolved.contains("pinot-json-1.0.jar"), resolved.toString());
    assertFalse(resolved.contains("kafka-clients-3.9.jar"), resolved.toString());
    assertFalse(resolved.contains("scala-library-2.13.jar"), resolved.toString());
  }

  /// Guards the specific regression the index exists to prevent: the shared store is never added
  /// as a directory, only through a selected plugin's index. A store jar no plugin declares must
  /// never appear.
  @Test
  public void testSharedStoreIsNeverShippedWholesale()
      throws Exception {
    File dist = buildDistribution();
    FileUtils.touch(new File(dist, "plugin-libs/orphan-not-declared-1.0.jar"));
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    List<String> resolved = names(cmd.resolveDepsJars(dist.getAbsolutePath()));
    assertFalse(resolved.contains("orphan-not-declared-1.0.jar"), resolved.toString());
  }

  /// A shared jar declared by two selected plugins must be shipped once.
  @Test
  public void testSharedDependencyIsNotDuplicated()
      throws Exception {
    File dist = buildDistribution();
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    cmd.setPluginsToLoad(List.of("pinot-json", "pinot-s3"));
    List<String> resolved = names(cmd.resolveDepsJars(dist.getAbsolutePath()));
    assertEquals(resolved.stream().filter("jackson-core-2.22.jar"::equals).count(), 1L, resolved.toString());
  }

  /// An old-format plugin keeps its jars beside its own jar and ships no index; it must still be
  /// shipped in full.
  @Test
  public void testOldFormatPluginWithoutIndexStillShipsItsOwnJars()
      throws Exception {
    File dist = buildDistribution();
    FileUtils.forceMkdir(new File(dist, "plugins/acme-thirdparty"));
    FileUtils.touch(new File(dist, "plugins/acme-thirdparty/acme-thirdparty-1.0.jar"));
    FileUtils.touch(new File(dist, "plugins/acme-thirdparty/acme-internal-dep-1.0.jar"));
    LaunchSparkDataIngestionJobCommand cmd = commandWithCliDefaults();
    List<String> resolved = names(cmd.resolveDepsJars(dist.getAbsolutePath()));
    assertTrue(resolved.contains("acme-thirdparty-1.0.jar"), resolved.toString());
    assertTrue(resolved.contains("acme-internal-dep-1.0.jar"), resolved.toString());
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
