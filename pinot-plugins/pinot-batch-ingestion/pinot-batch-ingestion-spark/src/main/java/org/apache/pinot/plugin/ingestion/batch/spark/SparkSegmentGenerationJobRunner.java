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
package org.apache.pinot.plugin.ingestion.batch.spark;

import static org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner.LOCAL_DIRECTORY_SEQUENCE_ID;
import static org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils.PINOT_PLUGINS_DIR;
import static org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils.PINOT_PLUGINS_TAR_GZ;
import static org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils.getFileName;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_DIR_PROPERTY_NAME;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_INCLUDE_PROPERTY_NAME;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils;
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
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkSegmentGenerationJobRunner implements IngestionJobRunner, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSegmentGenerationJobRunner.class);
  private static final String DEPS_JAR_DIR = "dependencyJarDir";
  private static final String STAGING_DIR = "stagingDir";

  private SegmentGenerationJobSpec _spec;

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
      String schemaURI = SegmentGenerationUtils
          .generateSchemaURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setSchemaURI(schemaURI);
    }
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils
          .generateTableConfigURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
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

    //Get pinotFS for input
    URI inputDirURI = new URI(_spec.getInputDirURI());
    if (inputDirURI.getScheme() == null) {
      inputDirURI = new File(_spec.getInputDirURI()).toURI();
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());

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
        throw new RuntimeException(String
            .format("The scheme of staging directory URI [%s] and output directory URI [%s] has to be same.",
                stagingDirURI, outputDirURI));
      }
      outputDirFS.mkdir(stagingDirURI);
    }
    //Get list of files to process
    String[] files = inputDirFS.listFiles(inputDirURI, true);

    //TODO: sort input files based on creation time
    List<String> filteredFiles = new ArrayList<>();
    PathMatcher includeFilePathMatcher = null;
    if (_spec.getIncludeFileNamePattern() != null) {
      includeFilePathMatcher = FileSystems.getDefault().getPathMatcher(_spec.getIncludeFileNamePattern());
    }
    PathMatcher excludeFilePathMatcher = null;
    if (_spec.getExcludeFileNamePattern() != null) {
      excludeFilePathMatcher = FileSystems.getDefault().getPathMatcher(_spec.getExcludeFileNamePattern());
    }

    for (String file : files) {
      if (includeFilePathMatcher != null) {
        if (!includeFilePathMatcher.matches(Paths.get(file))) {
          continue;
        }
      }
      if (excludeFilePathMatcher != null) {
        if (excludeFilePathMatcher.matches(Paths.get(file))) {
          continue;
        }
      }
      if (!inputDirFS.isDirectory(new URI(file))) {
        filteredFiles.add(file);
      }
    }

    LOGGER.info("Found {} files to create Pinot segments!", filteredFiles.size());
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
      if (getLocalDirectorySequenceId(_spec.getSegmentNameGeneratorSpec())) {
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
            pathAndIdxList.add(String.format("%s %d", siblingFiles.get(i), i));
          }
        }
      } else {
        for (int i = 0; i < filteredFiles.size(); i++) {
          pathAndIdxList.add(String.format("%s %d", filteredFiles.get(i), i));
        }
      }
      int numDataFiles = pathAndIdxList.size();
      int jobParallelism = _spec.getSegmentCreationJobParallelism();
      if (jobParallelism <= 0 || jobParallelism > numDataFiles) {
        jobParallelism = numDataFiles;
      }
      JavaRDD<String> pathRDD = sparkContext.parallelize(pathAndIdxList, jobParallelism);

      final String pluginsInclude =
          (sparkContext.getConf().contains(PLUGINS_INCLUDE_PROPERTY_NAME)) ? sparkContext.getConf()
              .get(PLUGINS_INCLUDE_PROPERTY_NAME) : null;
      final URI finalInputDirURI = inputDirURI;
      final URI finalOutputDirURI = (stagingDirURI == null) ? outputDirURI : stagingDirURI;
      // Prevent using lambda expression in Spark to avoid potential serialization exceptions, use inner function instead.
      pathRDD.foreach(new VoidFunction<String>() {
        @Override
        public void call(String pathAndIdx)
            throws Exception {
          PluginManager.get().init();
          for (PinotFSSpec pinotFSSpec : _spec.getPinotFSSpecs()) {
            PinotFSFactory
                .register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
          }
          PinotFS finalOutputDirFS = PinotFSFactory.create(finalOutputDirURI.getScheme());
          String[] splits = pathAndIdx.split(" ");
          String path = splits[0];
          int idx = Integer.valueOf(splits[1]);
          // Load Pinot Plugins copied from Distributed cache.
          File localPluginsTarFile = new File(PINOT_PLUGINS_TAR_GZ);
          if (localPluginsTarFile.exists()) {
            File pluginsDirFile = new File(PINOT_PLUGINS_DIR + "-" + idx);
            try {
              TarGzCompressionUtils.untar(localPluginsTarFile, pluginsDirFile);
            } catch (Exception e) {
              LOGGER.error("Failed to untar local Pinot plugins tarball file [{}]", localPluginsTarFile, e);
              throw new RuntimeException(e);
            }
            LOGGER.info("Trying to set System Property: [{}={}]", PLUGINS_DIR_PROPERTY_NAME,
                pluginsDirFile.getAbsolutePath());
            System.setProperty(PLUGINS_DIR_PROPERTY_NAME, pluginsDirFile.getAbsolutePath());
            if (pluginsInclude != null) {
              LOGGER.info("Trying to set System Property: [{}={}]", PLUGINS_INCLUDE_PROPERTY_NAME, pluginsInclude);
              System.setProperty(PLUGINS_INCLUDE_PROPERTY_NAME, pluginsInclude);
            }
            LOGGER.info("Pinot plugins System Properties are set at [{}], plugins includes [{}]",
                System.getProperty(PLUGINS_DIR_PROPERTY_NAME), System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME));
          } else {
            LOGGER.warn("Cannot find local Pinot plugins tar file at [{}]", localPluginsTarFile.getAbsolutePath());
          }
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
          taskSpec.setSchema(SegmentGenerationUtils.getSchema(_spec.getTableSpec().getSchemaURI()));
          taskSpec.setTableConfig(
              SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI()).toJsonNode());
          taskSpec.setSequenceId(idx);
          taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());
          taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

          SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
          String segmentName = taskRunner.run();

          // Tar segment directory to compress file
          File localSegmentDir = new File(localOutputTempDir, segmentName);
          String segmentTarFileName = segmentName + Constants.TAR_GZ_FILE_EXT;
          File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
          LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
          TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
          long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
          long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
          LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
              DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
          //move segment to output PinotFS
          URI outputSegmentTarURI =
              SegmentGenerationUtils.getRelativeOutputPath(finalInputDirURI, inputFileURI, finalOutputDirURI)
                  .resolve(segmentTarFileName);
          LOGGER.info("Trying to move segment tar file from: [{}] to [{}]", localSegmentTarFile, outputSegmentTarURI);
          if (!_spec.isOverwriteOutput() && PinotFSFactory.create(outputSegmentTarURI.getScheme())
              .exists(outputSegmentTarURI)) {
            LOGGER.warn("Not overwrite existing output segment tar file: {}",
                finalOutputDirFS.exists(outputSegmentTarURI));
          } else {
            finalOutputDirFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
          }
          FileUtils.deleteQuietly(localSegmentDir);
          FileUtils.deleteQuietly(localSegmentTarFile);
          FileUtils.deleteQuietly(localInputDataFile);
        }
      });
      if (stagingDirURI != null) {
        LOGGER.info("Trying to copy segment tars from staging directory: [{}] to output directory [{}]", stagingDirURI,
            outputDirURI);
        outputDirFS.copy(stagingDirURI, outputDirURI);
      }
    } finally {
      if (stagingDirURI != null) {
        LOGGER.info("Trying to clean up staging directory: [{}]", stagingDirURI);
        outputDirFS.delete(stagingDirURI, true);
      }
    }
  }

  private static boolean getLocalDirectorySequenceId(SegmentNameGeneratorSpec spec) {
    if (spec == null || spec.getConfigs() == null) {
      return false;
    }
    return Boolean.parseBoolean(spec.getConfigs().get(LOCAL_DIRECTORY_SEQUENCE_ID));
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

  protected void packPluginsToDistributedCache(JavaSparkContext sparkContext) {
    String pluginsRootDirPath = PluginManager.get().getPluginsRootDir();
    if (pluginsRootDirPath == null) {
      LOGGER.warn("Local Pinot plugins directory is null, skip packaging...");
      return;
    }
    File pluginsRootDir = new File(pluginsRootDirPath);
    if (pluginsRootDir.exists()) {
      File pluginsTarGzFile = new File(PINOT_PLUGINS_TAR_GZ);
      try {
        TarGzCompressionUtils.createTarGzFile(pluginsRootDir, pluginsTarGzFile);
      } catch (IOException e) {
        LOGGER.error("Failed to tar plugins directory", e);
      }
      sparkContext.addFile(pluginsTarGzFile.getAbsolutePath());
      String pluginsIncludes = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
      if (pluginsIncludes != null) {
        sparkContext.getConf().set(PLUGINS_INCLUDE_PROPERTY_NAME, pluginsIncludes);
      }
    } else {
      LOGGER.warn("Cannot find local Pinot plugins directory at [{}]", pluginsRootDirPath);
    }
  }
}
