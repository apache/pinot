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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationJobUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import static org.apache.pinot.common.segment.generation.SegmentGenerationUtils.PINOT_PLUGINS_DIR;
import static org.apache.pinot.common.segment.generation.SegmentGenerationUtils.PINOT_PLUGINS_TAR_GZ;
import static org.apache.pinot.common.segment.generation.SegmentGenerationUtils.getFileName;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_DIR_PROPERTY_NAME;
import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_INCLUDE_PROPERTY_NAME;


public class HadoopSegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentCreationMapper.class);
  protected static final String PROGRESS_REPORTER_THREAD_NAME = "pinot-hadoop-progress-reporter";
  protected static final long PROGRESS_REPORTER_JOIN_WAIT_TIME_MS = 5_000L;

  protected Configuration _jobConf;
  protected SegmentGenerationJobSpec _spec;
  private File _localTempDir;

  @Override
  public void setup(Context context)
      throws IOException {
    _jobConf = context.getConfiguration();
    Yaml yaml = new Yaml();
    String segmentGenerationJobSpecStr = _jobConf.get(HadoopSegmentGenerationJobRunner.SEGMENT_GENERATION_JOB_SPEC);
    _spec = yaml.loadAs(segmentGenerationJobSpecStr, SegmentGenerationJobSpec.class);
    LOGGER.info("Segment generation job spec : {}", segmentGenerationJobSpecStr);
    _localTempDir = new File(FileUtils.getTempDirectory(), "pinot-" + UUID.randomUUID());

    File localPluginsTarFile = new File(PINOT_PLUGINS_TAR_GZ);
    if (localPluginsTarFile.exists()) {
      File pluginsDirFile = Files.createTempDirectory(PINOT_PLUGINS_DIR).toFile();
      try {
        TarGzCompressionUtils.untar(localPluginsTarFile, pluginsDirFile);
      } catch (Exception e) {
        LOGGER.error("Failed to untar local Pinot plugins tarball file [{}]", localPluginsTarFile, e);
        throw new RuntimeException(e);
      }
      LOGGER.info("Trying to set System Property: {}={}", PLUGINS_DIR_PROPERTY_NAME, pluginsDirFile.getAbsolutePath());
      System.setProperty(PLUGINS_DIR_PROPERTY_NAME, pluginsDirFile.getAbsolutePath());
      String pluginsIncludes = _jobConf.get(PLUGINS_INCLUDE_PROPERTY_NAME);
      if (pluginsIncludes != null) {
        LOGGER.info("Trying to set System Property: {}={}", PLUGINS_INCLUDE_PROPERTY_NAME, pluginsIncludes);
        System.setProperty(PLUGINS_INCLUDE_PROPERTY_NAME, pluginsIncludes);
      }
      LOGGER.info("Pinot plugins System Properties are set at [{}], plugins includes [{}]",
          System.getProperty(PLUGINS_DIR_PROPERTY_NAME), System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME));
    } else {
      LOGGER.warn("Cannot find local Pinot plugins directory at [{}]", localPluginsTarFile.getAbsolutePath());
    }

    // Register file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) {
    try {
      String[] splits = StringUtils.split(value.toString(), ' ');
      Preconditions.checkState(splits.length == 2, "Illegal input value: %s", value);

      String path = splits[0];
      int idx = Integer.valueOf(splits[1]);
      LOGGER.info("Generating segment with input file: {}, sequence id: {}", path, idx);

      URI inputDirURI = new URI(_spec.getInputDirURI());
      if (inputDirURI.getScheme() == null) {
        inputDirURI = new File(_spec.getInputDirURI()).toURI();
      }

      URI outputDirURI = new URI(_spec.getOutputDirURI());
      if (outputDirURI.getScheme() == null) {
        outputDirURI = new File(_spec.getOutputDirURI()).toURI();
      }
      PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());

      URI inputFileURI = URI.create(path);
      if (inputFileURI.getScheme() == null) {
        inputFileURI =
            new URI(inputDirURI.getScheme(), inputFileURI.getSchemeSpecificPart(), inputFileURI.getFragment());
      }

      //create localTempDir for input and output
      File localInputTempDir = new File(_localTempDir, "input");
      FileUtils.forceMkdir(localInputTempDir);
      File localOutputTempDir = new File(_localTempDir, "output");
      FileUtils.forceMkdir(localOutputTempDir);

      //copy input path to local
      File localInputDataFile = new File(localInputTempDir, getFileName(inputFileURI));
      PinotFSFactory.create(inputFileURI.getScheme()).copyToLocalFile(inputFileURI, localInputDataFile);

      //create task spec
      SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
      taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());
      taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
      taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
      taskSpec.setSchema(SegmentGenerationUtils.getSchema(_spec.getTableSpec().getSchemaURI(), _spec.getAuthToken()));
      taskSpec.setTableConfig(
          SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), _spec.getAuthToken()));
      taskSpec.setSequenceId(idx);
      taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());
      taskSpec.setFailOnEmptySegment(_spec.isFailOnEmptySegment());
      taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

      // Start a thread that reports progress every minute during segment generation to prevent job getting killed
      Thread progressReporterThread = new Thread(getProgressReporter(context));
      progressReporterThread.setName(PROGRESS_REPORTER_THREAD_NAME);
      progressReporterThread.start();
      String segmentName;
      try {
        SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
        segmentName = taskRunner.run();
      } catch (Exception e) {
        LOGGER.error("Caught exception while creating segment with input file: {}, sequence id: {}", path, idx, e);
        throw new RuntimeException(e);
      } finally {
        progressReporterThread.interrupt();
        progressReporterThread.join(PROGRESS_REPORTER_JOIN_WAIT_TIME_MS);
        if (progressReporterThread.isAlive()) {
          LOGGER.error("Failed to interrupt progress reporter thread: {}", progressReporterThread);
        }
      }

      // Tar segment directory to compress file
      File localSegmentDir = new File(localOutputTempDir, segmentName);
      String segmentTarFileName = URLEncoder.encode(segmentName + Constants.TAR_GZ_FILE_EXT, "UTF-8");
      File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
      LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
      TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
      long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
      long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
      LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
          DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
      //move segment to output PinotFS
      URI relativeOutputPath = SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI);
      URI outputSegmentTarURI = relativeOutputPath.resolve(segmentTarFileName);
      SegmentGenerationJobUtils.moveLocalTarFileToRemote(localSegmentTarFile, outputSegmentTarURI,
          _spec.isOverwriteOutput());

      // Create and upload segment metadata tar file
      String metadataTarFileName = URLEncoder.encode(segmentName + Constants.METADATA_TAR_GZ_FILE_EXT, "UTF-8");
      URI outputMetadataTarURI = relativeOutputPath.resolve(metadataTarFileName);
      if (outputDirFS.exists(outputMetadataTarURI) && (_spec.isOverwriteOutput() || !_spec.isCreateMetadataTarGz())) {
        LOGGER.info("Deleting existing metadata tar gz file: {}", outputMetadataTarURI);
        outputDirFS.delete(outputMetadataTarURI, true);
      }

      if (taskSpec.isCreateMetadataTarGz()) {
        File localMetadataTarFile = new File(localOutputTempDir, metadataTarFileName);
        SegmentGenerationJobUtils.createSegmentMetadataTarGz(localSegmentDir, localMetadataTarFile);
        SegmentGenerationJobUtils.moveLocalTarFileToRemote(localMetadataTarFile, outputMetadataTarURI,
            _spec.isOverwriteOutput());
      }
      FileUtils.deleteQuietly(localSegmentDir);
      FileUtils.deleteQuietly(localInputDataFile);

      context.write(new LongWritable(idx), new Text(segmentTarFileName));
      LOGGER.info("Finish generating segment: {} with input file: {}, sequence id: {}", segmentName, inputFileURI, idx);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(_localTempDir);
    }
  }

  protected Runnable getProgressReporter(Context context) {
    return new ProgressReporter(context);
  }

  @Override
  public void cleanup(Context context) {
    LOGGER.info("Deleting local temporary directory: {}", _localTempDir);
    FileUtils.deleteQuietly(_localTempDir);
  }

  private static class ProgressReporter implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressReporter.class);
    private static final long PROGRESS_REPORTER_INTERVAL_MS = 60_000L;

    private final Context _context;

    ProgressReporter(Context context) {
      _context = context;
    }

    @Override
    public void run() {
      LOGGER.info("Starting progress reporter thread: {}", Thread.currentThread());
      while (true) {
        try {
          Thread.sleep(PROGRESS_REPORTER_INTERVAL_MS);
          LOGGER.info("============== Reporting progress ==============");
          _context.progress();
        } catch (InterruptedException e) {
          LOGGER.info("Progress reporter thread: {} interrupted", Thread.currentThread());
          return;
        }
      }
    }
  }
}
