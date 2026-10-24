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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationJobUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.plexus.classworlds.realm.NoSuchRealmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.segment.generation.SegmentGenerationUtils.getFileName;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_DIR_PROPERTY_NAME;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_INCLUDE_PROPERTY_NAME;


public class SparkSegmentGenerationJobRunner implements IngestionJobRunner, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSegmentGenerationJobRunner.class);
  private static final String DEPS_JAR_DIR = "dependencyJarDir";
  private static final String STAGING_DIR = "stagingDir";

  private SegmentGenerationJobSpec _spec;
  private String _pluginsArchiveName;
  private String _pluginsArchiveDigest;
  private String _pluginsInclude;

  public SparkSegmentGenerationJobRunner() {
  }

  public SparkSegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getInputDirURI() == null) {
      throw new RuntimeException("Missing property 'inputDirURI' in 'jobSpec' file");
    }
    if (_spec.getOutputDirURI() == null) {
      throw new RuntimeException("Missing property 'outputDirURI' in 'jobSpec' file");
    }
    if (_spec.getRecordReaderSpec() == null) {
      throw new RuntimeException("Missing property 'recordReaderSpec' in 'jobSpec' file");
    }
    if (_spec.getTableSpec() == null) {
      throw new RuntimeException("Missing property 'tableSpec' in 'jobSpec' file");
    }
    if (_spec.getTableSpec().getTableName() == null) {
      throw new RuntimeException("Missing property 'tableName' in 'tableSpec'");
    }
    if (_spec.getTableSpec().getSchemaURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'schemaURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String schemaURI = SegmentGenerationUtils.generateSchemaURI(pinotClusterSpec.getControllerURI(),
          _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setSchemaURI(schemaURI);
    }
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils.generateTableConfigURI(pinotClusterSpec.getControllerURI(),
          _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }
    if (_spec.getExecutionFrameworkSpec().getExtraConfigs() == null) {
      _spec.getExecutionFrameworkSpec().setExtraConfigs(new HashMap<>());
    }
  }

  @Override
  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }
    //Get list of files to process
    URI inputDirURI = new URI(_spec.getInputDirURI());
    if (inputDirURI.getScheme() == null) {
      inputDirURI = new File(_spec.getInputDirURI()).toURI();
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());
    List<String> filteredFiles = SegmentGenerationUtils.listMatchedFilesWithRecursiveOption(inputDirFS, inputDirURI,
        _spec.getIncludeFileNamePattern(), _spec.getExcludeFileNamePattern(), _spec.isSearchRecursively());
    LOGGER.info("Found {} files to create Pinot segments!", filteredFiles.size());

    TableConfig tableConfig =
        SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), _spec.getAuthToken());
    boolean consistentPushEnabled = ConsistentDataPushUtils.consistentDataPushEnabled(tableConfig);

    if (consistentPushEnabled) {
      ConsistentDataPushUtils.configureSegmentPostfix(_spec);
    }

    //Get outputFS for writing output pinot segments
    URI outputDirURI = new URI(_spec.getOutputDirURI());
    if (outputDirURI.getScheme() == null) {
      outputDirURI = new File(_spec.getOutputDirURI()).toURI();
    }
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    outputDirFS.mkdir(outputDirURI);

    //Get staging directory for temporary output pinot segments
    String stagingDir = _spec.getExecutionFrameworkSpec().getExtraConfigs().get(STAGING_DIR);
    URI stagingDirURI = null;
    if (stagingDir != null) {
      stagingDirURI = URI.create(stagingDir);
      if (stagingDirURI.getScheme() == null) {
        stagingDirURI = new File(stagingDir).toURI();
      }
      if (!outputDirURI.getScheme().equals(stagingDirURI.getScheme())) {
        throw new RuntimeException("The scheme of staging directory URI [" + stagingDirURI + "] and output directory "
            + "URI [" + outputDirURI + "] has to be same.");
      }
      outputDirFS.mkdir(stagingDirURI);
    }
    try {
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());

      // Pinot plugins are necessary to launch Pinot ingestion job from every mapper.
      // In order to ensure pinot plugins would be loaded to each worker, this method
      // tars entire plugins directory and set this file into Distributed cache.
      // Then each executor job will untar the plugin tarball, and set system properties accordingly.
      packPluginsToDistributedCache(sparkContext);

      // Add dependency jars
      if (_spec.getExecutionFrameworkSpec().getExtraConfigs().containsKey(DEPS_JAR_DIR)) {
        addDepsJarToDistributedCache(sparkContext,
            _spec.getExecutionFrameworkSpec().getExtraConfigs().get(DEPS_JAR_DIR));
      }

      List<String> pathAndIdxList = new ArrayList<>();
      if (!SegmentGenerationJobUtils.useGlobalDirectorySequenceId(_spec.getSegmentNameGeneratorSpec())) {
        Map<String, List<String>> localDirIndex = new HashMap<>();
        for (String filteredFile : filteredFiles) {
          Path filteredParentPath = Paths.get(filteredFile).getParent();
          if (!localDirIndex.containsKey(filteredParentPath.toString())) {
            localDirIndex.put(filteredParentPath.toString(), new ArrayList<>());
          }
          localDirIndex.get(filteredParentPath.toString()).add(filteredFile);
        }
        for (String parentPath : localDirIndex.keySet()) {
          List<String> siblingFiles = localDirIndex.get(parentPath);
          Collections.sort(siblingFiles);
          for (int i = 0; i < siblingFiles.size(); i++) {
            pathAndIdxList.add(siblingFiles.get(i) + " " + i);
          }
        }
      } else {
        for (int i = 0; i < filteredFiles.size(); i++) {
          pathAndIdxList.add(filteredFiles.get(i) + " " + i);
        }
      }
      int numDataFiles = pathAndIdxList.size();
      int jobParallelism = _spec.getSegmentCreationJobParallelism();
      if (jobParallelism <= 0 || jobParallelism > numDataFiles) {
        jobParallelism = numDataFiles;
      }
      JavaRDD<String> pathRDD = sparkContext.parallelize(pathAndIdxList, jobParallelism);

      final String pluginsInclude = _pluginsInclude;
      final URI finalInputDirURI = inputDirURI;
      final URI finalOutputDirURI = (stagingDirURI == null) ? outputDirURI : stagingDirURI;
      // Prevent using lambda expression in Spark to avoid potential serialization exceptions, use inner function
      // instead.
      pathRDD.foreach(new VoidFunction<String>() {
        @Override
        public void call(String pathAndIdx)
            throws Exception {
          String[] splits = pathAndIdx.split(" ");
          String path = splits[0];
          int idx = Integer.valueOf(splits[1]);
          File archive = _pluginsArchiveName == null ? null : new File(SparkFiles.get(_pluginsArchiveName));
          bootstrapPlugins(archive, _pluginsArchiveDigest, pluginsInclude, _spec,
              new File(FileUtils.getTempDirectory(), "pinot-spark-plugins-" + _pluginsArchiveDigest));
          PinotFS finalOutputDirFS = PinotFSFactory.create(finalOutputDirURI.getScheme());
          URI inputFileURI = URI.create(path);
          if (inputFileURI.getScheme() == null) {
            inputFileURI =
                new URI(finalInputDirURI.getScheme(), inputFileURI.getSchemeSpecificPart(), inputFileURI.getFragment());
          }

          //create localTempDir for input and output
          File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-" + UUID.randomUUID());
          File localInputTempDir = new File(localTempDir, "input");
          FileUtils.forceMkdir(localInputTempDir);
          File localOutputTempDir = new File(localTempDir, "output");
          FileUtils.forceMkdir(localOutputTempDir);

          //copy input path to local
          File localInputDataFile = new File(localInputTempDir, getFileName(inputFileURI));
          LOGGER.info("Trying to copy input file from {} to {}", inputFileURI, localInputDataFile);
          PinotFSFactory.create(inputFileURI.getScheme()).copyToLocalFile(inputFileURI, localInputDataFile);

          //create task spec
          SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
          taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());
          taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
          taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
          taskSpec.setSchema(
              SegmentGenerationUtils.getSchema(_spec.getTableSpec().getSchemaURI(), _spec.getAuthToken(),
                  _spec.getTlsSpec()));
          taskSpec.setTableConfig(
              SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), _spec.getAuthToken(),
                  _spec.getTlsSpec()));
          taskSpec.setSequenceId(idx);
          taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());
          taskSpec.setFailOnEmptySegment(_spec.isFailOnEmptySegment());
          taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

          SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
          String segmentName = taskRunner.run();

          // Tar segment directory to compress file
          File localSegmentDir = new File(localOutputTempDir, segmentName);
          String segmentTarFileName = URIUtils.encode(segmentName + Constants.TAR_GZ_FILE_EXT);
          File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
          LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
          TarCompressionUtils.createCompressedTarFile(localSegmentDir, localSegmentTarFile);
          long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
          long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
          LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
              DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
          // Move segment to output PinotFS
          URI relativeOutputPath =
              SegmentGenerationUtils.getRelativeOutputPath(finalInputDirURI, inputFileURI, finalOutputDirURI);
          URI outputSegmentTarURI = relativeOutputPath.resolve(segmentTarFileName);
          SegmentGenerationJobUtils.moveLocalTarFileToRemote(localSegmentTarFile, outputSegmentTarURI,
              _spec.isOverwriteOutput());

          // Create and upload segment metadata tar file
          String metadataTarFileName = URIUtils.encode(segmentName + Constants.METADATA_TAR_GZ_FILE_EXT);
          URI outputMetadataTarURI = relativeOutputPath.resolve(metadataTarFileName);
          if (finalOutputDirFS.exists(outputMetadataTarURI) && (_spec.isOverwriteOutput()
              || !_spec.isCreateMetadataTarGz())) {
            LOGGER.info("Deleting existing metadata tar gz file: {}", outputMetadataTarURI);
            finalOutputDirFS.delete(outputMetadataTarURI, true);
          }
          if (taskSpec.isCreateMetadataTarGz()) {
            File localMetadataTarFile = new File(localOutputTempDir, metadataTarFileName);
            SegmentGenerationJobUtils.createSegmentMetadataTarGz(localSegmentDir, localMetadataTarFile);
            SegmentGenerationJobUtils.moveLocalTarFileToRemote(localMetadataTarFile, outputMetadataTarURI,
                _spec.isOverwriteOutput());
          }
          FileUtils.deleteQuietly(localSegmentDir);
          FileUtils.deleteQuietly(localInputDataFile);
        }
      });
      if (stagingDirURI != null) {
        LOGGER.info("Trying to move segment tars from staging directory: [{}] to output directory [{}]", stagingDirURI,
            outputDirURI);
        SegmentGenerationJobUtils.moveFiles(outputDirFS, stagingDirURI, outputDirURI, true);
      }
    } finally {
      if (stagingDirURI != null) {
        LOGGER.info("Trying to clean up staging directory: [{}]", stagingDirURI);
        outputDirFS.delete(stagingDirURI, true);
      }
    }
  }

  protected void addDepsJarToDistributedCache(JavaSparkContext sparkContext, String depsJarDir)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir);
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir).toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      for (String file : files) {
        if (!pinotFS.isDirectory(URI.create(file))) {
          if (file.endsWith(".jar")) {
            LOGGER.info("Adding deps jar: {} to distributed cache", file);
            sparkContext.addJar(file);
          }
        }
      }
    }
  }

  // Plugin topology is established before generation. Cooperative bootstraps share the SPI lock even when
  // Spark loads this runner twice. Immutable installed jars are retained for the lifetime of their loaders.
  private static String _lastPluginPayload;
  private static Map<String, Class<?>> _lastPluginClasses = Map.of();

  static void bootstrapPlugins(File archive, String expectedDigest, String include, SegmentGenerationJobSpec spec,
      File installDir)
      throws Exception {
    synchronized (PluginManager.class) {
      if (archive == null) {
        PluginManager.get().init();
      } else {
        List<String> requirements = new ArrayList<>();
        requirements.add(spec.getRecordReaderSpec().getClassName());
        requirements.add(spec.getRecordReaderSpec().getConfigClassName());
        spec.getPinotFSSpecs().forEach(fs -> requirements.add(fs.getClassName()));
        String key = archive.getCanonicalPath() + ":" + expectedDigest + ":" + include + ":" + requirements
            + ":" + archive.length() + ":" + archive.lastModified();
        // This is only a fast path for our immutable installation. Recheck actual registry identities, not a
        // runner-local success flag. A different runner classloader independently verifies source provenance.
        if (!key.equals(_lastPluginPayload) || !samePluginClasses(_lastPluginClasses)) {
          if (!expectedDigest.equals(fileDigest(archive))) {
            throw new IOException("Distributed Pinot plugin archive digest does not match " + archive);
          }
          installPluginArchive(archive, installDir);
          String previousDir = System.getProperty(PLUGINS_DIR_PROPERTY_NAME);
          String previousInclude = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
          PluginManager manager;
          try {
            System.setProperty(PLUGINS_DIR_PROPERTY_NAME, installDir.getAbsolutePath());
            restoreProperty(PLUGINS_INCLUDE_PROPERTY_NAME, include);
            manager = PluginManager.get();
          } finally {
            restoreProperty(PLUGINS_DIR_PROPERTY_NAME, previousDir);
            restoreProperty(PLUGINS_INCLUDE_PROPERTY_NAME, previousInclude);
          }
          Map<String, File> selected = manager.getPluginsToLoad(installDir.getAbsolutePath(), include);
          if (selected.isEmpty()) {
            throw new IOException("Distributed Pinot plugin archive contains no selected plugins");
          }
          Map<String, Class<?>> verified = new HashMap<>();
          Map<String, Set<Path>> sources = new HashMap<>();
          List<String> absent = new ArrayList<>();
          // Check every existing name before changing the registry. Never overwrite a legacy loader or
          // duplicate a new-style realm. An opaque/partially published name requires a fresh executor.
          for (Map.Entry<String, File> plugin : selected.entrySet()) {
            if (pluginPresent(manager, plugin.getKey())) {
              verifyPluginSource(manager, plugin.getKey(), plugin.getValue(), verified, sources);
            } else {
              absent.add(plugin.getKey());
            }
          }
          try {
            for (String name : absent) {
              manager.load(name, selected.get(name));
              verifyPluginSource(manager, name, selected.get(name), verified, sources);
            }
            verifyCapability(manager, spec.getRecordReaderSpec().getClassName(), RecordReader.class, verified, sources);
            verifyCapability(manager, spec.getRecordReaderSpec().getConfigClassName(), RecordReaderConfig.class,
                verified, sources);
            for (PinotFSSpec fs : spec.getPinotFSSpecs()) {
              verifyCapability(manager, fs.getClassName(), PinotFS.class, verified, sources);
            }
          } catch (Exception | LinkageError e) {
            throw new IllegalStateException("Distributed Pinot plugin capability failed; use a fresh executor "
                + "for failed or partially loaded plugins", e);
          }
          _lastPluginClasses = Map.copyOf(verified);
          _lastPluginPayload = key;
        }
      }
      for (PinotFSSpec fs : spec.getPinotFSSpecs()) {
        PinotFSFactory.registerIfNeeded(fs.getScheme(), fs.getClassName(), new PinotConfiguration(fs));
      }
    }
  }

  private static boolean samePluginClasses(Map<String, Class<?>> classes)
      throws ClassNotFoundException {
    for (Map.Entry<String, Class<?>> entry : classes.entrySet()) {
      if (PluginManager.get().loadClass(entry.getKey()) != entry.getValue()) {
        return false;
      }
    }
    return true;
  }

  private static void verifyCapability(PluginManager manager, String name, Class<?> contract,
      Map<String, Class<?>> verified, Map<String, Set<Path>> sources)
      throws Exception {
    if (name != null) {
      Class<?> implementation = manager.loadClass(name);
      if (!contract.isAssignableFrom(implementation)) {
        throw new IllegalStateException("Pinot plugin does not implement shared " + contract.getName() + ": " + name);
      }
      int separator = name.indexOf(':');
      if (separator > 0) {
        String prefix = name.substring(0, separator + 1);
        Set<Path> allowed = sources.get(name.substring(0, separator));
        if (allowed != null && (implementation.getProtectionDomain().getCodeSource() == null
            || !allowed.contains(Paths.get(implementation.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toRealPath()))) {
          throw new IllegalStateException("Pinot plugin class source is outside its verified payload: " + name);
        }
        for (Map.Entry<String, Class<?>> probe : verified.entrySet()) {
          if (probe.getKey().startsWith(prefix)
              && probe.getValue().getClassLoader() != implementation.getClassLoader()) {
            throw new IllegalStateException("Pinot plugin class resolves outside its payload: " + name);
          }
        }
      }
      verified.put(name, implementation);
    }
  }

  private static boolean pluginPresent(PluginManager manager, String name)
      throws ClassNotFoundException {
    try {
      manager.loadClass(name, "java.lang.Object");
      return true;
    } catch (RuntimeException e) {
      if (e.getCause() instanceof NoSuchRealmException) {
        return false;
      }
      throw e;
    }
  }

  private static void verifyPluginSource(PluginManager manager, String name, File expected,
      Map<String, Class<?>> verified, Map<String, Set<Path>> sources)
      throws Exception {
    Path expectedRoot = expected.getCanonicalFile().toPath();
    String fingerprint = pluginFingerprint(expectedRoot.toFile());
    // New-style realms put every immediate entry on the classpath, including directories and non-JAR archives.
    // Legacy loaders use recursive JARs only. Compare the complete loader URLs and all backing content.
    List<File> entries = new ArrayList<>();
    if (Files.isRegularFile(expectedRoot.resolve("pinot-plugin.properties"))) {
      try (Stream<Path> paths = Files.list(expectedRoot)) {
        paths.sorted().forEach(path -> entries.add(path.toFile()));
      }
    } else {
      entries.addAll(FileUtils.listFiles(expectedRoot.toFile(), new String[]{"jar"}, true));
    }
    Set<Path> expectedEntries = entries.stream().map(f -> expectedRoot.relativize(f.toPath()))
        .collect(Collectors.toSet());
    for (File entry : entries) {
      Path relativeSource = expectedRoot.relativize(entry.toPath());
      List<String> classes = pluginClassNames(entry);
      for (String className : classes) {
        Class<?> loaded;
        try {
          loaded = manager.loadClass(name, className);
        } catch (ClassNotFoundException | LinkageError e) {
          continue;
        }
        if (!(loaded.getClassLoader() instanceof URLClassLoader)
            || loaded.getProtectionDomain().getCodeSource() == null) {
          continue;
        }
        Path source = Paths.get(loaded.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        Path actualRoot = source;
        for (int i = 0; i < relativeSource.getNameCount() && actualRoot != null; i++) {
          actualRoot = actualRoot.getParent();
        }
        if (actualRoot == null || !actualRoot.resolve(relativeSource).equals(source)) {
          continue;
        }
        Set<Path> actualEntries = new HashSet<>();
        Set<Path> actualSources = new HashSet<>();
        for (URL url : ((URLClassLoader) loaded.getClassLoader()).getURLs()) {
          Path path = Paths.get(url.toURI()).toRealPath();
          actualEntries.add(actualRoot.relativize(path));
          actualSources.add(path);
        }
        if (actualEntries.equals(expectedEntries) && fingerprint.equals(pluginFingerprint(actualRoot.toFile()))) {
          verified.put(name + ":" + className, loaded);
          sources.put(name, Set.copyOf(actualSources));
          return;
        }
      }
    }
    throw new IllegalStateException("Cannot verify immutable Pinot plugin '" + name
        + "' against the distributed payload; use a fresh executor for changed or partially loaded plugins");
  }

  private static List<String> pluginClassNames(File entry)
      throws IOException {
    List<String> names = new ArrayList<>();
    if (entry.isDirectory()) {
      try (Stream<Path> files = Files.walk(entry.toPath())) {
        files.filter(Files::isRegularFile).forEach(file -> names.add(entry.toPath().relativize(file).toString()
            .replace(File.separatorChar, '/')));
      }
    } else {
      try (JarFile archive = new JarFile(entry)) {
        for (JarEntry file : Collections.list(archive.entries())) {
          names.add(file.getName());
        }
      } catch (ZipException e) {
        // A realm also lists non-archive resources such as its descriptor. Their bytes remain in the fingerprint.
        return List.of();
      }
    }
    return names.stream().map(n -> n.replaceFirst("^META-INF/versions/[0-9]+/", ""))
        .filter(n -> n.endsWith(".class") && !n.startsWith("META-INF/") && !n.equals("module-info.class"))
        .map(n -> n.substring(0, n.length() - ".class".length()).replace('/', '.')).distinct().sorted()
        .collect(Collectors.toList());
  }

  private static String pluginFingerprint(File directory)
      throws IOException {
    Path root = directory.toPath();
    List<Path> files;
    try (Stream<Path> paths = Files.walk(root)) {
      files = paths.filter(Files::isRegularFile).sorted(Comparator.comparing(root::relativize))
          .collect(Collectors.toList());
    }
    StringBuilder identity = new StringBuilder();
    for (Path file : files) {
      identity.append(root.relativize(file)).append('\0').append(fileDigest(file.toFile())).append('\0');
    }
    return DigestUtils.sha256Hex(identity.toString());
  }

  private static String fileDigest(File file)
      throws IOException {
    try (InputStream input = Files.newInputStream(file.toPath())) {
      return DigestUtils.sha256Hex(input);
    }
  }

  private static void installPluginArchive(File archive, File installDir)
      throws IOException {
    if (installDir.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(installDir.getParentFile());
    Path staged = Files.createTempDirectory(installDir.getParentFile().toPath(), "pinot-plugin-install-");
    try {
      TarCompressionUtils.untar(archive, staged.toFile());
      Files.move(staged, installDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
    } finally {
      FileUtils.deleteDirectory(staged.toFile());
    }
  }

  private static void restoreProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  protected void packPluginsToDistributedCache(JavaSparkContext sparkContext) {
    _pluginsArchiveName = null;
    _pluginsArchiveDigest = null;
    _pluginsInclude = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
    if (_pluginsInclude == null && sparkContext.getConf().contains(PLUGINS_INCLUDE_PROPERTY_NAME)) {
      _pluginsInclude = sparkContext.getConf().get(PLUGINS_INCLUDE_PROPERTY_NAME);
    }
    String[] pluginDirectories = PluginManager.get().getPluginsDirectories();
    if (pluginDirectories == null) {
      LOGGER.warn("Plugin directories is null, skipping packaging...");
      return;
    }

    ArrayList<File> validPluginDirectories = new ArrayList<>();

    for (String pluginsDirPath : pluginDirectories) {
      File pluginsDir = new File(pluginsDirPath);
      if (pluginsDir.exists()) {
        validPluginDirectories.add(pluginsDir);
      } else {
        LOGGER.warn("Cannot find Pinot plugins directory at [{}]", pluginsDirPath);
        return;
      }
    }

    try {
      File distribution = Files.createTempDirectory("pinot-spark-plugin-distribution-").toFile();
      distribution.deleteOnExit();
      File archive = new File(distribution, "plugins.tar.gz");
      TarCompressionUtils.createCompressedTarFile(validPluginDirectories.toArray(new File[0]), archive);
      _pluginsArchiveDigest = fileDigest(archive);
      _pluginsArchiveName = "pinot-plugins-" + _pluginsArchiveDigest + ".tar.gz";
      File namedArchive = new File(distribution, _pluginsArchiveName);
      Files.move(archive.toPath(), namedArchive.toPath());
      namedArchive.deleteOnExit();
      sparkContext.addFile(namedArchive.getAbsolutePath());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to distribute Pinot plugin payload", e);
    }
  }
}
