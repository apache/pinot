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
package org.apache.pinot.plugin.ingestion.batch.hadoop;

import static org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils.PINOT_PLUGINS_TAR_GZ;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_INCLUDE_PROPERTY_NAME;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Preconditions;


public class HadoopSegmentGenerationJobRunner extends Configured implements IngestionJobRunner, Serializable {

  public static final String SEGMENT_GENERATION_JOB_SPEC = "segmentGenerationJobSpec";
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentGenerationJobRunner.class);
  private static final String DEPS_JAR_DIR = "dependencyJarDir";
  private static final String STAGING_DIR = "stagingDir";
  private static final String SEGMENT_TAR_DIR = "segmentTar";

  private SegmentGenerationJobSpec _spec;

  public HadoopSegmentGenerationJobRunner() {
    setConf(new org.apache.hadoop.conf.Configuration());
    getConf().set("mapreduce.job.user.classpath.first", "true");
  }

  public HadoopSegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {
    this();
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
      new PinotConfiguration(pinotFSSpec);
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
    Preconditions.checkNotNull(stagingDir, "Please set config: stagingDir under 'executionFrameworkSpec.extraConfigs'");
    URI stagingDirURI = URI.create(stagingDir);
    if (stagingDirURI.getScheme() == null) {
      stagingDirURI = new File(stagingDir).toURI();
    }
    if (!outputDirURI.getScheme().equals(stagingDirURI.getScheme())) {
      throw new RuntimeException(String
          .format("The scheme of staging directory URI [%s] and output directory URI [%s] has to be same.",
              stagingDirURI, outputDirURI));
    }
    outputDirFS.mkdir(stagingDirURI);
    Path stagingInputDir = new Path(stagingDirURI.toString(), "input");
    outputDirFS.mkdir(stagingInputDir.toUri());
    Path stagingSegmentTarUri = new Path(stagingDirURI.toString(), SEGMENT_TAR_DIR);
    outputDirFS.mkdir(stagingSegmentTarUri.toUri());

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

    int numDataFiles = filteredFiles.size();
    if (numDataFiles == 0) {
      String errorMessage = String
          .format("No data file founded in [%s], with include file pattern: [%s] and exclude file  pattern [%s]",
              _spec.getInputDirURI(), _spec.getIncludeFileNamePattern(), _spec.getExcludeFileNamePattern());
      LOGGER.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      LOGGER.info("Creating segments with data files: {}", filteredFiles);
      for (int i = 0; i < numDataFiles; i++) {
        String dataFilePath = filteredFiles.get(i);

        File localFile = new File("tmp");
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(localFile))) {
          dataOutputStream.write(StringUtil.encodeUtf8(dataFilePath + " " + i));
          dataOutputStream.flush();
          outputDirFS.copyFromLocalFile(localFile, new Path(stagingInputDir, Integer.toString(i)).toUri());
        }
      }
    }

    try {
      // Set up the job
      Job job = Job.getInstance(getConf());
      job.setJarByClass(getClass());
      job.setJobName(getClass().getName());

      org.apache.hadoop.conf.Configuration jobConf = job.getConfiguration();
      String hadoopTokenFileLocation = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
      if (hadoopTokenFileLocation != null) {
        jobConf.set("mapreduce.job.credentials.binary", hadoopTokenFileLocation);
      }
      jobConf.setInt(JobContext.NUM_MAPS, numDataFiles);

      // Pinot plugins are necessary to launch Pinot ingestion job from every mapper.
      // In order to ensure pinot plugins would be loaded to each worker, this method
      // tars entire plugins directory and set this file into Distributed cache.
      // Then each mapper job will untar the plugin tarball, and set system properties accordingly.
      packPluginsToDistributedCache(job);

      // Add dependency jars
      if (_spec.getExecutionFrameworkSpec().getExtraConfigs().containsKey(DEPS_JAR_DIR)) {
        addDepsJarToDistributedCache(job, _spec.getExecutionFrameworkSpec().getExtraConfigs().get(DEPS_JAR_DIR));
      }

      _spec.setOutputDirURI(stagingSegmentTarUri.toUri().toString());
      jobConf.set(SEGMENT_GENERATION_JOB_SPEC, new Yaml().dump(_spec));
      _spec.setOutputDirURI(outputDirURI.toString());

      job.setMapperClass(getMapperClass());
      job.setNumReduceTasks(0);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, stagingInputDir);
      FileOutputFormat.setOutputPath(job, new Path(stagingDir, "output"));

      // Submit the job
      job.waitForCompletion(true);
      if (!job.isSuccessful()) {
        throw new RuntimeException("Job failed: " + job);
      }

      LOGGER.info("Trying to copy segment tars from staging directory: [{}] to output directory [{}]", stagingDirURI,
          outputDirURI);
      outputDirFS.copy(new Path(stagingDir, SEGMENT_TAR_DIR).toUri(), outputDirURI);
    } finally {
      LOGGER.info("Trying to clean up staging directory: [{}]", stagingDirURI);
      outputDirFS.delete(stagingDirURI, true);
    }
  }

  /**
   * Can be overridden to plug in custom mapper.
   */
  protected Class<? extends Mapper<LongWritable, Text, LongWritable, Text>> getMapperClass() {
    return HadoopSegmentCreationMapper.class;
  }

  protected void packPluginsToDistributedCache(Job job) {
    String pluginsRootDir = PluginManager.get().getPluginsRootDir();
    if (new File(pluginsRootDir).exists()) {
      File pluginsTarGzFile = new File(PINOT_PLUGINS_TAR_GZ);
      try {
        TarGzCompressionUtils.createTarGzOfDirectory(pluginsRootDir, pluginsTarGzFile.getPath());
      } catch (IOException e) {
        LOGGER.error("Failed to tar plugins directory", e);
        throw new RuntimeException(e);
      }
      job.addCacheArchive(pluginsTarGzFile.toURI());

      String pluginsIncludes = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
      if (pluginsIncludes != null) {
        job.getConfiguration().set(PLUGINS_INCLUDE_PROPERTY_NAME, pluginsIncludes);
      }
    } else {
      LOGGER.warn("Cannot find local Pinot plugins directory at [{}]", pluginsRootDir);
    }
  }

  protected void addDepsJarToDistributedCache(Job job, String depsJarDir)
      throws IOException {
    if (depsJarDir != null) {
      URI depsJarDirURI = URI.create(depsJarDir);
      if (depsJarDirURI.getScheme() == null) {
        depsJarDirURI = new File(depsJarDir).toURI();
      }
      PinotFS pinotFS = PinotFSFactory.create(depsJarDirURI.getScheme());
      String[] files = pinotFS.listFiles(depsJarDirURI, true);
      for (String file : files) {
        URI fileURI = URI.create(file);
        if (!pinotFS.isDirectory(fileURI)) {
          if (file.endsWith(".jar")) {
            LOGGER.info("Adding deps jar: {} to distributed cache", file);
            job.addCacheArchive(fileURI);
          }
        }
      }
    }
  }
}
