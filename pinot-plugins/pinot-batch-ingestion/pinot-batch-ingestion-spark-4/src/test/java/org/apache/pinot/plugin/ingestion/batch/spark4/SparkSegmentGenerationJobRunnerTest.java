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
package org.apache.pinot.plugin.ingestion.batch.spark4;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SparkSegmentGenerationJobRunnerTest {
  private SparkContext _sparkContext;

  @BeforeClass
  public void setup() {
    // Spark 4's web UI (Jetty 12 / Jakarta Servlet) conflicts with Pinot's transitive Jersey 2.x
    // on the test classpath. The UI is not needed for these tests, so disable it.
    // Pin the driver to 127.0.0.1 so the test does not depend on hostname resolution (macOS
    // CI hosts often return a hostname that does not resolve back to a bindable address,
    // producing "Can't assign requested address" with Spark 4.1+).
    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName(SparkSegmentGenerationJobRunnerTest.class.getName())
        .set("spark.ui.enabled", "false")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.host", "127.0.0.1");
    _sparkContext = new SparkContext(conf);
  }

  private SegmentGenerationJobSpec setupAppendTableSpec(File testDir)
      throws Exception {
    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File outputDir = new File(testDir, "output");
    final String outputFilename = "myTable_OFFLINE_0.tar.gz";
    FileUtils.touch(new File(outputDir, outputFilename));
    final String existingFilename = "myTable_OFFLINE_100.tar.gz";
    FileUtils.touch(new File(outputDir, existingFilename));

    final String schemaName = "myTable";
    File schemaFile = new File(testDir, "myTable.schema");
    Schema schema = new SchemaBuilder().setSchemaName(schemaName).addSingleValueDimension("col1", DataType.STRING)
        .addMetric("col2", DataType.INT).build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    File tableConfigFile = new File(testDir, "myTable.table");
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setNumReplicas(1).build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    jobSpec.setOverwriteOutput(false);

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName(CSVRecordReader.class.getName());
    recordReaderSpec.setConfigClassName(CSVRecordReaderConfig.class.getName());
    jobSpec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
    jobSpec.setTableSpec(tableSpec);

    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    efSpec.setName("standalone");
    efSpec.setSegmentGenerationJobRunnerClassName(SparkSegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    return jobSpec;
  }

  private SegmentGenerationJobSpec setupRefreshTableSpec(File testDir)
      throws Exception {
    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();

    // Create two input files
    File inputFile1 = new File(inputDir, "input1.csv");
    File inputFile2 = new File(inputDir, "input2.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));

    File outputDir = new File(testDir, "output");
    final String schemaName = "myTable";
    File schemaFile = new File(testDir, "myTable.schema");
    Schema schema = new SchemaBuilder().setSchemaName(schemaName).addSingleValueDimension("col1", DataType.STRING)
        .addMetric("col2", DataType.INT).build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", true));
    File tableConfigFile = new File(testDir, "myTable.table");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setNumReplicas(1)
        .setIngestionConfig(ingestionConfig).build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    jobSpec.setOverwriteOutput(false);

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName(CSVRecordReader.class.getName());
    recordReaderSpec.setConfigClassName(CSVRecordReaderConfig.class.getName());
    jobSpec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
    jobSpec.setTableSpec(tableSpec);

    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    efSpec.setName("standalone");
    efSpec.setSegmentGenerationJobRunnerClassName(SparkSegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    return jobSpec;
  }

  @DataProvider(name = "distributedPlugins")
  public Object[][] distributedPlugins() {
    return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
  }

  @Test(dataProvider = "distributedPlugins")
  public void testDistributedPluginBootstrap(boolean newStyle, boolean initialized)
      throws Exception {
    File root = Files.createTempDirectory("spark-plugin-bootstrap-").toFile();
    File original = createPluginArchive(new File(root, "original"), newStyle, "first");
    File changed = createPluginArchive(new File(root, "changed"), newStyle, "second");
    System.out.println("BOOTSTRAP_FIXTURE_ROOT " + root);
    runBootstrapFork(root, original, changed, initialized, "first");
    if (!initialized) {
      runBootstrapFork(root, changed, original, false, "second");
    }
    if (newStyle && initialized) {
      File firstDirectory = createDirectoryPluginArchive(new File(root, "directory-first"), "first", null);
      File sharedJar = new File(root, "directory-first/plugins/payload/payload.jar");
      File secondDirectory = createDirectoryPluginArchive(new File(root, "directory-second"), "second", sharedJar);
      assertEquals(archiveDigest(sharedJar),
          archiveDigest(new File(root, "directory-second/plugins/payload/payload.jar")));
      runBootstrapFork(root, firstDirectory, secondDirectory, true, "first");
      runBootstrapFork(root, secondDirectory, firstDirectory, false, "second");
    }
  }

  private static String testClasspath() {
    return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
  }

  private static File createPluginArchive(File root, boolean newStyle, String marker)
      throws Exception {
    File sources = new File(root, "sources/payloadonly");
    File classes = new File(root, "classes");
    File plugins = new File(root, "plugins");
    File plugin = new File(plugins, "payload");
    FileUtils.forceMkdir(sources);
    FileUtils.forceMkdir(classes);
    FileUtils.forceMkdir(plugin);
    File reader = new File(sources, "Reader.java");
    File fs = new File(sources, "FS.java");
    FileUtils.writeStringToFile(reader, "package payloadonly; "
        + "public class Reader extends org.apache.pinot.plugin.inputformat.csv.CSVRecordReader { "
        + "public org.apache.pinot.spi.data.readers.GenericRow next("
        + "org.apache.pinot.spi.data.readers.GenericRow row) throws java.io.IOException { "
        + "super.next(row); row.putValue(\"payload\", \"" + marker + "\"); return row; }}", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(fs, "package payloadonly; "
        + "public class FS extends org.apache.pinot.spi.filesystem.LocalPinotFS { "
        + "public static final java.util.concurrent.atomic.AtomicInteger INITS = "
        + "new java.util.concurrent.atomic.AtomicInteger(); "
        + "public void init(org.apache.pinot.spi.env.PinotConfiguration c) { INITS.incrementAndGet(); }}",
        StandardCharsets.UTF_8);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertNotNull(compiler);
    assertEquals(compiler.run(null, null, null, "-classpath", testClasspath(), "-d", classes.getAbsolutePath(),
        reader.getAbsolutePath(), fs.getAbsolutePath()), 0, "Payload fixture compilation must succeed");
    try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(new File(plugin, "payload.jar").toPath()))) {
      for (String name : List.of("Reader", "FS")) {
        File compiled = new File(classes, "payloadonly/" + name + ".class");
        assertTrue(compiled.isFile());
        jar.putNextEntry(new JarEntry("payloadonly/" + name + ".class"));
        Files.copy(compiled.toPath(), jar);
        jar.closeEntry();
      }
    }
    if (newStyle) {
      FileUtils.writeStringToFile(new File(plugin, "pinot-plugin.properties"), "parent.realmId=pinot\n",
          StandardCharsets.UTF_8);
    }
    File archive = new File(root, "payload.tar.gz");
    TarCompressionUtils.createCompressedTarFile(new File[]{plugins}, archive);
    assertTrue(archive.length() > 0);
    return archive;
  }

  private static File createDirectoryPluginArchive(File root, String marker, File sharedJar)
      throws Exception {
    File archive = createPluginArchive(root, true, marker);
    File plugin = new File(root, "plugins/payload");
    File jar = new File(plugin, "payload.jar");
    if (sharedJar == null) {
      try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar.toPath()))) {
        JarEntry entry = new JarEntry("payloadonly/FS.class");
        entry.setTime(0);
        out.putNextEntry(entry);
        Files.copy(new File(root, "classes/payloadonly/FS.class").toPath(), out);
        out.closeEntry();
      }
    } else {
      FileUtils.copyFile(sharedJar, jar);
    }
    File directoryClass = new File(plugin, "classes/payloadonly/Reader.class");
    FileUtils.forceMkdir(directoryClass.getParentFile());
    FileUtils.copyFile(new File(root, "classes/payloadonly/Reader.class"), directoryClass);
    TarCompressionUtils.createCompressedTarFile(new File[]{plugin.getParentFile()}, archive);
    return archive;
  }

  private static File createPartialArchive(File original)
      throws Exception {
    File root = new File(original.getParentFile(), "partial");
    File extracted = new File(root, "source");
    TarCompressionUtils.untar(original, extracted);
    File jar = FileUtils.listFiles(extracted, new String[]{"jar"}, true).iterator().next();
    File plugins = new File(root, "plugins");
    File broken = new File(plugins, "broken");
    FileUtils.forceMkdir(broken);
    try (JarFile source = new JarFile(jar);
        JarOutputStream out = new JarOutputStream(Files.newOutputStream(new File(broken, "partial.jar").toPath()))) {
      out.putNextEntry(new JarEntry("payloadonly/FS.class"));
      try (InputStream bytes = source.getInputStream(source.getJarEntry("payloadonly/FS.class"))) {
        bytes.transferTo(out);
      }
      out.closeEntry();
    }
    File descriptor = new File(jar.getParentFile(), "pinot-plugin.properties");
    if (descriptor.isFile()) {
      FileUtils.copyFile(descriptor, new File(broken, descriptor.getName()));
    }
    File archive = new File(root, "partial.tar.gz");
    TarCompressionUtils.createCompressedTarFile(new File[]{plugins}, archive);
    return archive;
  }

  private static void runBootstrapFork(File root, File archive, File changed, boolean initialized, String marker)
      throws Exception {
    File log = new File(root, "fork-" + initialized + "-" + marker + ".log");
    Process process = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
        "-cp", testClasspath(), SparkSegmentGenerationJobRunnerTest.class.getName(), archive.getAbsolutePath(),
        changed.getAbsolutePath(), Boolean.toString(initialized), marker)
        .redirectErrorStream(true).redirectOutput(log).start();
    boolean finished = process.waitFor(60, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
    }
    assertTrue(finished, "Bootstrap fork must terminate: " + log);
    String output = Files.readString(log.toPath());
    System.out.println(output);
    assertEquals(process.exitValue(), 0, "Real executor bootstrap/copy/read capability: " + output);
  }

  /// Fresh-JVM executor bootstrap entry. It never resets a private PluginManager singleton.
  public static void main(String[] args)
      throws Exception {
    File archive = new File(args[0]);
    File changed = new File(args[1]);
    assertTrue(archive.isFile());
    System.out.println("BOOTSTRAP_ENV java=" + System.getProperty("java.version")
        + " manager=" + PluginManager.class.getProtectionDomain().getCodeSource().getLocation()
        + " readerSPI=" + RecordReader.class.getProtectionDomain().getCodeSource().getLocation()
        + " fsSPI=" + PinotFS.class.getProtectionDomain().getCodeSource().getLocation()
        + " classpath=" + System.getProperty("java.class.path"));
    for (String name : List.of("payloadonly.Reader", "payloadonly.FS")) {
      expectThrows(ClassNotFoundException.class,
          () -> Class.forName(name, false, SparkSegmentGenerationJobRunnerTest.class.getClassLoader()));
    }
    if (Boolean.parseBoolean(args[2])) {
      PluginManager.get().init();
    }
    SegmentGenerationJobSpec spec = pluginSpec();
    String digest = archiveDigest(archive);
    File install = new File(archive.getParentFile(), "installed-" + digest);
    // A missing archive fails before first plugin mutation; a subsequent valid bootstrap still works.
    expectThrows(IOException.class, () -> SparkSegmentGenerationJobRunner.bootstrapPlugins(
        new File(archive.getParentFile(), "missing.tar.gz"), digest, "payload", spec, install));
    bootstrapAndRead(archive, digest, spec, install, args[3]);
    Class<?> reader = PluginManager.get().loadClass("payload:payloadonly.Reader");
    Class<?> fs = PluginManager.get().loadClass("payload:payloadonly.FS");
    Object registered = PinotFSFactory.create("payload");
    int realmCount = PluginManager.get().getPluginClassLoaders().size();
    bootstrapAndRead(archive, digest, spec, install, args[3]);
    ExecutorService calls = Executors.newFixedThreadPool(2);
    try {
      List<Future<Class<?>>> repeats = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        repeats.add(calls.submit(() -> bootstrapFromLoader(archive, digest, spec, install)));
      }
      for (Future<Class<?>> repeat : repeats) {
        assertSame(repeat.get(), reader);
      }
    } finally {
      calls.shutdownNow();
    }
    // A second runner/test classloader shares the same SPI anchor. Its empty local cache cannot cause a reload.
    URL[] runnerUrls = {SparkSegmentGenerationJobRunner.class.getProtectionDomain().getCodeSource().getLocation(),
        SparkSegmentGenerationJobRunnerTest.class.getProtectionDomain().getCodeSource().getLocation()};
    try (URLClassLoader other = new URLClassLoader(runnerUrls,
        SparkSegmentGenerationJobRunnerTest.class.getClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve)
          throws ClassNotFoundException {
        if (name.startsWith(SparkSegmentGenerationJobRunner.class.getName())
            || name.startsWith(SparkSegmentGenerationJobRunnerTest.class.getName())) {
          synchronized (getClassLoadingLock(name)) {
            Class<?> result = findLoadedClass(name);
            if (result == null) {
              result = findClass(name);
            }
            if (resolve) {
              resolveClass(result);
            }
            return result;
          }
        }
        return super.loadClass(name, resolve);
      }
    }) {
      assertSame(other.loadClass(PluginManager.class.getName()), PluginManager.class);
      Class<?> bridge = other.loadClass(SparkSegmentGenerationJobRunnerTest.class.getName());
      assertSame(bridge.getMethod("bootstrapFromLoader", File.class, String.class, SegmentGenerationJobSpec.class,
          File.class).invoke(null, archive, digest, spec, install), reader);
    }
    assertSame(PinotFSFactory.create("payload"), registered);
    assertEquals(((AtomicInteger) fs.getField("INITS").get(null)).get(), 1);
    assertEquals(PluginManager.get().getPluginClassLoaders().size(), realmCount);
    expectThrows(IllegalStateException.class, () -> SparkSegmentGenerationJobRunner.bootstrapPlugins(changed,
        archiveDigest(changed), "payload", spec, new File(changed.getParentFile(), "installed-changed")));
    assertSame(PluginManager.get().loadClass("payload:payloadonly.Reader"), reader);
    assertSame(PinotFSFactory.create("payload"), registered);
    bootstrapAndRead(archive, digest, spec, install, args[3]);
    SegmentGenerationJobSpec changedCapability = pluginSpec();
    changedCapability.getRecordReaderSpec().setClassName("payload:payloadonly.MissingReader");
    expectThrows(IllegalStateException.class, () -> SparkSegmentGenerationJobRunner.bootstrapPlugins(archive,
        digest, "payload", changedCapability, install));
    assertSame(PinotFSFactory.create("payload"), registered);
    File equivalent = new File(archive.getParentFile(), "equivalent/payload-copy.tar.gz");
    FileUtils.copyFile(archive, equivalent);
    SparkSegmentGenerationJobRunner.bootstrapPlugins(equivalent, digest, "payload", spec,
        new File(equivalent.getParentFile(), "installed"));
    assertSame(PluginManager.get().loadClass("payload:payloadonly.Reader"), reader);
    assertSame(PinotFSFactory.create("payload"), registered);
    File broken = createPartialArchive(archive);
    SegmentGenerationJobSpec incomplete = pluginSpec();
    incomplete.getRecordReaderSpec().setClassName("broken:payloadonly.MissingReader");
    incomplete.getPinotFSSpecs().get(0).setScheme("broken");
    incomplete.getPinotFSSpecs().get(0).setClassName("broken:payloadonly.FS");
    File brokenInstall = new File(broken.getParentFile(), "installed");
    expectThrows(IllegalStateException.class, () -> SparkSegmentGenerationJobRunner.bootstrapPlugins(broken,
        archiveDigest(broken), "broken", incomplete, brokenInstall));
    Class<?> partial = PluginManager.get().loadClass("broken:payloadonly.FS");
    int partialRealms = PluginManager.get().getPluginClassLoaders().size();
    expectThrows(IllegalStateException.class, () -> SparkSegmentGenerationJobRunner.bootstrapPlugins(broken,
        archiveDigest(broken), "broken", incomplete, brokenInstall));
    assertSame(PluginManager.get().loadClass("broken:payloadonly.FS"), partial);
    assertEquals(PluginManager.get().getPluginClassLoaders().size(), partialRealms);
    assertFalse(PinotFSFactory.isSchemeSupported("broken"),
        "Failed capability must not publish filesystem registration");
    System.out.println("BOOTSTRAP_PROOF marker=" + args[3] + " initialized=" + args[2] + " digest=" + digest
        + " readerSource=" + reader.getProtectionDomain().getCodeSource().getLocation()
        + " fsSource=" + fs.getProtectionDomain().getCodeSource().getLocation() + " fsInits=1 realms=" + realmCount);
  }

  /// Public bridge for a second runner loader sharing the existing PluginManager/SPI classes.
  public static Class<?> bootstrapFromLoader(File archive, String digest, SegmentGenerationJobSpec spec, File install)
      throws Exception {
    SparkSegmentGenerationJobRunner.bootstrapPlugins(archive, digest, "payload", spec, install);
    return PluginManager.get().loadClass("payload:payloadonly.Reader");
  }

  private static SegmentGenerationJobSpec pluginSpec() {
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    RecordReaderSpec reader = new RecordReaderSpec();
    reader.setClassName("payload:payloadonly.Reader");
    spec.setRecordReaderSpec(reader);
    PinotFSSpec fs = new PinotFSSpec();
    fs.setScheme("payload");
    fs.setClassName("payload:payloadonly.FS");
    spec.setPinotFSSpecs(List.of(fs));
    return spec;
  }

  private static String archiveDigest(File archive)
      throws IOException {
    try (InputStream input = Files.newInputStream(archive.toPath())) {
      return DigestUtils.sha256Hex(input);
    }
  }

  private static void bootstrapAndRead(File archive, String digest, SegmentGenerationJobSpec spec, File install,
      String marker)
      throws Exception {
    SparkSegmentGenerationJobRunner.bootstrapPlugins(archive, digest, "payload", spec, install);
    File source = new File(archive.getParentFile(), "input.csv");
    FileUtils.writeStringToFile(source, "value\nactual-row\n", StandardCharsets.UTF_8);
    File copied = new File(archive.getParentFile(), "copied.csv");
    PinotFSFactory.create("payload").copyToLocalFile(source.toURI(), copied);
    assertEquals(Files.readString(copied.toPath()), Files.readString(source.toPath()));
    try (RecordReader reader = RecordReaderFactory.getRecordReaderByClass(
        spec.getRecordReaderSpec().getClassName(), copied, null, new CSVRecordReaderConfig())) {
      assertTrue(reader.hasNext());
      GenericRow row = reader.next();
      assertEquals(row.getValue("value"), "actual-row");
      assertEquals(row.getValue("payload"), marker);
      assertFalse(reader.hasNext());
    }
  }

  @Test
  public void testExecutorFileSystemsAreReused()
      throws Exception {
    File testDir = Files.createTempDirectory("spark-file-system-reuse-").toFile();
    SegmentGenerationJobSpec spec = setupAppendTableSpec(testDir);
    spec.setOverwriteOutput(true);
    for (int i = 1; i < 3; i++) {
      FileUtils.writeLines(new File(testDir, "input/input" + i + ".csv"),
          List.of("col1,col2", "value" + i + "," + i));
    }
    PinotFSSpec fsSpec = spec.getPinotFSSpecs().get(0);
    fsSpec.setClassName(CountingPinotFS.class.getName());
    fsSpec.setConfigs(Map.of("job", "first"));
    CountingPinotFS.INIT_COUNT.set(0);
    CountingPinotFS.COPY_INSTANCES.clear();
    try {
      new SparkSegmentGenerationJobRunner(spec).run();
      assertEquals(CountingPinotFS.INIT_COUNT.get(), 1, "Per-file executor setup must reuse the driver registration");
      assertEquals(CountingPinotFS.COPY_INSTANCES.size(), 1);
      PinotFS first = PinotFSFactory.create("file");
      for (int i = 0; i < 3; i++) {
        assertTrue(new File(testDir, "output/myTable_OFFLINE_" + i + ".tar.gz").length() > 0);
      }
      fsSpec.setConfigs(Map.of("job", "second"));
      new SparkSegmentGenerationJobRunner(spec).run();
      assertEquals(CountingPinotFS.INIT_COUNT.get(), 2,
          "A new explicit job registration must replace its configuration");
      assertEquals(CountingPinotFS.COPY_INSTANCES.size(), 2);
      assertNotSame(PinotFSFactory.create("file"), first);
      assertTrue(first.exists(new File(testDir, "input/input.csv").toURI()), "A borrowed previous client stays usable");
    } finally {
      PinotFSFactory.register("file", LocalPinotFS.class.getName(), new PinotConfiguration());
      FileUtils.deleteDirectory(testDir);
    }
  }

  /// Counts actual client creation and input copies across local executor tasks.
  public static class CountingPinotFS extends LocalPinotFS {
    static final AtomicInteger INIT_COUNT = new AtomicInteger();
    static final Set<CountingPinotFS> COPY_INSTANCES = ConcurrentHashMap.newKeySet();

    @Override
    public void init(PinotConfiguration configuration) {
      INIT_COUNT.incrementAndGet();
    }

    @Override
    public void copyToLocalFile(URI source, File destination)
        throws Exception {
      COPY_INSTANCES.add(this);
      super.copyToLocalFile(source, destination);
    }
  }

  @Test
  public void testSegmentGeneration()
      throws Exception {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();

    SegmentGenerationJobSpec jobSpec = setupAppendTableSpec(testDir);

    SparkSegmentGenerationJobRunner jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    final String outputFilename = "myTable_OFFLINE_0.tar.gz";
    final String existingFilename = "myTable_OFFLINE_100.tar.gz";
    File outputDir = new File(testDir, "output");

    File oldSegmentFile = new File(outputDir, existingFilename);
    Assert.assertTrue(oldSegmentFile.exists());

    // The output directory should have the original file in it (since we aren't overwriting)
    File newSegmentFile = new File(outputDir, outputFilename);
    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() == 0);

    // Now run again, but this time with overwriting of output files, and confirm we got a valid segment file.
    jobSpec.setOverwriteOutput(true);
    jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // The original file should still be there.
    Assert.assertTrue(oldSegmentFile.exists());

    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() > 0);

    // FUTURE - validate contents of file?
  }

  @Test
  public void testSegmentGenerationWithConsistentPush()
      throws Exception {
    File testDir = Files.createTempDirectory("testSegmentGenerationWithConsistentPush-").toFile();
    testDir.delete();
    testDir.mkdirs();

    SegmentGenerationJobSpec jobSpec = setupRefreshTableSpec(testDir);
    SparkSegmentGenerationJobRunner jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // Assert that the segment name generator spec is present and has the correct value
    Assert.assertNotNull(jobSpec.getSegmentNameGeneratorSpec());
    Assert.assertEquals(jobSpec.getSegmentNameGeneratorSpec().getConfigs().keySet().size(), 1);
    Assert.assertTrue(jobSpec.getSegmentNameGeneratorSpec().getConfigs().containsKey("segment.name.postfix"));
    // Value should be the current time but we can't predict it, so just check that it's a number
    Assert.assertTrue(jobSpec.getSegmentNameGeneratorSpec().getConfigs().get("segment.name.postfix").matches("\\d+"));
    String segmentNamePostfix = jobSpec.getSegmentNameGeneratorSpec().getConfigs().get("segment.name.postfix");

    String expectedSegmentName0 = "myTable_OFFLINE_" + segmentNamePostfix + "_0.tar.gz";
    String expectedSegmentName1 = "myTable_OFFLINE_" + segmentNamePostfix + "_1.tar.gz";

    // Get list of files in the output directory and assert segment name
    File outputDir = new File(testDir, "output");
    File[] files = outputDir.listFiles();
    Set<String> fileNames = Arrays.stream(files).map(File::getName).collect(Collectors.toSet());
    Set<String> expectedNames = Set.of(expectedSegmentName0, expectedSegmentName1);
    Assert.assertEquals(fileNames, expectedNames);
  }

  @Test
  public void testInputFilesWithSameNameInDifferentDirectories()
      throws Exception {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();

    File inputDir = new File(testDir, "input");
    File inputSubDir1 = new File(inputDir, "2009");
    File inputSubDir2 = new File(inputDir, "2010");
    inputSubDir1.mkdirs();
    inputSubDir2.mkdirs();

    File inputFile1 = new File(inputSubDir1, "input.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir2, "input.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));

    File outputDir = new File(testDir, "output");

    // Set up schema file.
    final String schemaName = "mySchema";
    File schemaFile = new File(testDir, "schema");
    Schema schema = new SchemaBuilder().setSchemaName(schemaName).addSingleValueDimension("col1", DataType.STRING)
        .addMetric("col2", DataType.INT).build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    // Set up table config file.
    File tableConfigFile = new File(testDir, "tableConfig");
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setNumReplicas(1).build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setSearchRecursively(true);
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    jobSpec.setOverwriteOutput(true);

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName(CSVRecordReader.class.getName());
    recordReaderSpec.setConfigClassName(CSVRecordReaderConfig.class.getName());
    jobSpec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
    jobSpec.setTableSpec(tableSpec);

    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    efSpec.setName("standalone");
    efSpec.setSegmentGenerationJobRunnerClassName(SparkSegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    SparkSegmentGenerationJobRunner jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // Check that both segment files are created

    File newSegmentFile2009 = new File(outputDir, "2009/myTable_OFFLINE_0.tar.gz");
    Assert.assertTrue(newSegmentFile2009.exists());
    Assert.assertTrue(newSegmentFile2009.isFile());
    Assert.assertTrue(newSegmentFile2009.length() > 0);

    File newSegmentFile2010 = new File(outputDir, "2010/myTable_OFFLINE_0.tar.gz");
    Assert.assertTrue(newSegmentFile2010.exists());
    Assert.assertTrue(newSegmentFile2010.isFile());
    Assert.assertTrue(newSegmentFile2010.length() > 0);
  }
}
