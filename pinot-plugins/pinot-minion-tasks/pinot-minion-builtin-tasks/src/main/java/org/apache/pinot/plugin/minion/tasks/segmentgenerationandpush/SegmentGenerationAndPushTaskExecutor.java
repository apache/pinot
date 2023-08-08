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
package org.apache.pinot.plugin.minion.tasks.segmentgenerationandpush;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentGenerationAndPushTaskExecutor implements a minion task to build a single Pinot segment based on one input file
 * given task configs.
 *
 * Task configs:
 *   input.data.file.uri - Required, the location of input file
 *   inputFormat - Required, the input file format, e.g. JSON/Avro/Parquet/CSV/...
 *   input.fs.className - Optional, the class name of filesystem to read input data. Default to PinotLocalFs if not
 *   specified.
 *   input.fs.prop.<keys> - Optional, defines the configs to initialize input filesystem.
 *
 *   output.segment.dir.uri -  Optional, the location of generated segment. Use local temp dir with push mode TAR, If
 *   not specified.
 *   output.fs.className - Optional, the class name of filesystem to write output segment. Default to PinotLocalFs if
 *   not specified.
 *   output.fs.prop.<keys> - Optional, the configs to initialize output filesystem.
 *   overwriteOutput - Optional, delete the output segment directory if set to true.
 *
 *   recordReader.className - Required, the class name of RecordReader.
 *   recordReader.configClassName - Required, the class name of RecordReaderConfig.
 *   recordReader.prop.<keys> - Optional, the configs used to initialize RecordReaderConfig.
 *
 *   schema - Required, if schemaURI is not specified. Pinot schema in Json string.
 *   schemaURI - Required, if schema is not specified. The URI to query for Pinot schema.
 *
 *   sequenceId - Optional, an option to set segment name.
 *   segmentNameGenerator.type - Required, the segment name generator to create segment name.
 *   segmentNameGenerator.configs.<keys> - Optional, configs of segment name generator.
 *
 *   push.mode - Required, push job type: TAR/URI/METADATA
 *   push.controllerUri - Required, controller uri to send push request to.
 *   push.segmentUriPrefix - Optional, segment download uri prefix, used when push.mode=uri
 *   push.segmentUriSuffix - Optional, segment download uri suffix, used when push.mode=uri
 *
 */
public class SegmentGenerationAndPushTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationAndPushTaskExecutor.class);

  private static final int DEFUALT_PUSH_ATTEMPTS = 5;
  private static final int DEFAULT_PUSH_PARALLELISM = 1;
  private static final long DEFAULT_PUSH_RETRY_INTERVAL_MILLIS = 1000L;

  private PinotTaskConfig _pinotTaskConfig;
  private MinionEventObserver _eventObserver;

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    LOGGER.info("Executing SegmentGenerationAndPushTask with task config: {}", pinotTaskConfig);
    Map<String, String> taskConfigs = pinotTaskConfig.getConfigs();
    SegmentGenerationAndPushResult.Builder resultBuilder = new SegmentGenerationAndPushResult.Builder();
    File localTempDir = new File(new File(MinionContext.getInstance().getDataDir(), "SegmentGenerationAndPushResult"),
        "tmp-" + UUID.randomUUID());
    _pinotTaskConfig = pinotTaskConfig;
    _eventObserver = MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());
    try {
      SegmentGenerationTaskSpec taskSpec = generateTaskSpec(taskConfigs, localTempDir);
      return generateAndPushSegment(taskSpec, resultBuilder, taskConfigs);
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute SegmentGenerationAndPushTask", e);
    } finally {
      // Cleanup output dir
      FileUtils.deleteQuietly(localTempDir);
    }
  }

  private SegmentGenerationAndPushResult generateAndPushSegment(SegmentGenerationTaskSpec taskSpec,
      SegmentGenerationAndPushResult.Builder resultBuilder, Map<String, String> taskConfigs)
      throws Exception {
    // Generate Pinot Segment
    _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segment");
    SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
    String segmentName = taskRunner.run();

    // Tar segment directory to compress file
    _eventObserver.notifyProgress(_pinotTaskConfig, "Compressing segment: " + segmentName);
    File localSegmentTarFile = tarSegmentDir(taskSpec, segmentName);

    //move segment to output PinotFS
    _eventObserver.notifyProgress(_pinotTaskConfig, String.format("Moving segment: %s to output dir", segmentName));
    URI outputSegmentTarURI = moveSegmentToOutputPinotFS(taskConfigs, localSegmentTarFile);
    LOGGER.info("Moved generated segment from [{}] to location: [{}]", localSegmentTarFile, outputSegmentTarURI);

    resultBuilder.setSegmentName(segmentName);
    // Segment push task
    // TODO: Make this use SegmentUploader
    _eventObserver.notifyProgress(_pinotTaskConfig, "Pushing segment: " + segmentName);
    pushSegment(taskSpec.getTableConfig().getTableName(), taskConfigs, outputSegmentTarURI);
    resultBuilder.setSucceed(true);

    return resultBuilder.build();
  }

  private void pushSegment(String tableName, Map<String, String> taskConfigs, URI outputSegmentTarURI)
      throws Exception {
    String pushMode = taskConfigs.get(BatchConfigProperties.PUSH_MODE);
    LOGGER.info("Trying to push Pinot segment with push mode {} from {}", pushMode, outputSegmentTarURI);

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(DEFUALT_PUSH_ATTEMPTS);
    pushJobSpec.setPushParallelism(DEFAULT_PUSH_PARALLELISM);
    pushJobSpec.setPushRetryIntervalMillis(DEFAULT_PUSH_RETRY_INTERVAL_MILLIS);
    pushJobSpec.setSegmentUriPrefix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX));
    pushJobSpec.setSegmentUriSuffix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_SUFFIX));

    SegmentGenerationJobSpec spec = generatePushJobSpec(tableName, taskConfigs, pushJobSpec);

    URI outputSegmentDirURI = null;
    if (taskConfigs.containsKey(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI)) {
      outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    }
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
      switch (BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase())) {
        case TAR:
          try (PinotFS pinotFS = MinionTaskUtils.getLocalPinotFs()) {
            SegmentPushUtils.pushSegments(spec, pinotFS, Arrays.asList(outputSegmentTarURI.toString()));
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        case URI:
          try {
            List<String> segmentUris = new ArrayList<>();
            URI updatedURI = SegmentPushUtils.generateSegmentTarURI(outputSegmentDirURI, outputSegmentTarURI,
                pushJobSpec.getSegmentUriPrefix(), pushJobSpec.getSegmentUriSuffix());
            segmentUris.add(updatedURI.toString());
            SegmentPushUtils.sendSegmentUris(spec, segmentUris);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        case METADATA:
          try {
            Map<String, String> segmentUriToTarPathMap =
                SegmentPushUtils.getSegmentUriToTarPathMap(outputSegmentDirURI, pushJobSpec,
                    new String[]{outputSegmentTarURI.toString()});
            SegmentPushUtils.sendSegmentUriAndMetadata(spec, outputFileFS, segmentUriToTarPathMap);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized push mode - " + pushMode);
      }
    }
  }

  private SegmentGenerationJobSpec generatePushJobSpec(String tableName, Map<String, String> taskConfigs,
      PushJobSpec pushJobSpec) {

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(tableName);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(taskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI));
    PinotClusterSpec[] pinotClusterSpecs = new PinotClusterSpec[]{pinotClusterSpec};

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setPushJobSpec(pushJobSpec);
    spec.setTableSpec(tableSpec);
    spec.setPinotClusterSpecs(pinotClusterSpecs);
    spec.setAuthToken(taskConfigs.get(BatchConfigProperties.AUTH_TOKEN));

    return spec;
  }

  private URI moveSegmentToOutputPinotFS(Map<String, String> taskConfigs, File localSegmentTarFile)
      throws Exception {
    if (!taskConfigs.containsKey(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI)) {
      return localSegmentTarFile.toURI();
    }
    URI outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
      URI outputSegmentTarURI = URI.create(outputSegmentDirURI + localSegmentTarFile.getName());
      if (!Boolean.parseBoolean(taskConfigs.get(BatchConfigProperties.OVERWRITE_OUTPUT)) && outputFileFS.exists(
          outputSegmentDirURI)) {
        LOGGER.warn("Not overwrite existing output segment tar file: {}", outputFileFS.exists(outputSegmentDirURI));
      } else {
        outputFileFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
      }
      return outputSegmentTarURI;
    }
  }

  private File tarSegmentDir(SegmentGenerationTaskSpec taskSpec, String segmentName)
      throws IOException {
    File localOutputTempDir = new File(taskSpec.getOutputDirectoryPath());
    File localSegmentDir = new File(localOutputTempDir, segmentName);
    String segmentTarFileName = segmentName + Constants.TAR_GZ_FILE_EXT;
    File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
    LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
    TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
    long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
    long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
    LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
        DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
    return localSegmentTarFile;
  }

  protected SegmentGenerationTaskSpec generateTaskSpec(Map<String, String> taskConfigs, File localTempDir)
      throws Exception {
    SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
    URI inputFileURI = URI.create(taskConfigs.get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY));

    try (PinotFS inputFileFS = MinionTaskUtils.getInputPinotFS(taskConfigs, inputFileURI)) {
      File localInputTempDir = new File(localTempDir, "input");
      FileUtils.forceMkdir(localInputTempDir);
      File localOutputTempDir = new File(localTempDir, "output");
      FileUtils.forceMkdir(localOutputTempDir);
      taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());

      //copy input path to local
      _eventObserver.notifyProgress(_pinotTaskConfig, String.format("Copying file: %s to local disk", inputFileURI));
      File localInputDataFile = new File(localInputTempDir, new File(inputFileURI.getPath()).getName());
      inputFileFS.copyToLocalFile(inputFileURI, localInputDataFile);
      taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());

      RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
      recordReaderSpec.setDataFormat(taskConfigs.get(BatchConfigProperties.INPUT_FORMAT));
      recordReaderSpec.setClassName(taskConfigs.get(BatchConfigProperties.RECORD_READER_CLASS));
      recordReaderSpec.setConfigClassName(taskConfigs.get(BatchConfigProperties.RECORD_READER_CONFIG_CLASS));
      taskSpec.setRecordReaderSpec(recordReaderSpec);

      String authToken = taskConfigs.get(BatchConfigProperties.AUTH_TOKEN);

      String tableNameWithType = taskConfigs.get(BatchConfigProperties.TABLE_NAME);
      Schema schema;
      if (taskConfigs.containsKey(BatchConfigProperties.SCHEMA)) {
        schema = JsonUtils.stringToObject(JsonUtils.objectToString(taskConfigs.get(BatchConfigProperties.SCHEMA)),
            Schema.class);
      } else if (taskConfigs.containsKey(BatchConfigProperties.SCHEMA_URI)) {
        schema = SegmentGenerationUtils.getSchema(taskConfigs.get(BatchConfigProperties.SCHEMA_URI), authToken);
      } else {
        schema = getSchema(tableNameWithType);
      }
      taskSpec.setSchema(schema);
      TableConfig tableConfig;
      if (taskConfigs.containsKey(BatchConfigProperties.TABLE_CONFIGS)) {
        tableConfig = JsonUtils.stringToObject(taskConfigs.get(BatchConfigProperties.TABLE_CONFIGS), TableConfig.class);
      } else if (taskConfigs.containsKey(BatchConfigProperties.TABLE_CONFIGS_URI)) {
        tableConfig = SegmentGenerationUtils.getTableConfig(
            taskConfigs.get(BatchConfigProperties.TABLE_CONFIGS_URI), authToken);
      } else {
        tableConfig = getTableConfig(tableNameWithType);
      }
      taskSpec.setTableConfig(tableConfig);
      taskSpec.setSequenceId(Integer.parseInt(taskConfigs.get(BatchConfigProperties.SEQUENCE_ID)));
      if (taskConfigs.containsKey(BatchConfigProperties.FAIL_ON_EMPTY_SEGMENT)) {
        taskSpec.setFailOnEmptySegment(
            Boolean.parseBoolean(taskConfigs.get(BatchConfigProperties.FAIL_ON_EMPTY_SEGMENT)));
      }
      SegmentNameGeneratorSpec segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
      segmentNameGeneratorSpec.setType(taskConfigs.get(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE));
      segmentNameGeneratorSpec.setConfigs(IngestionConfigUtils.getConfigMapWithPrefix(taskConfigs,
          BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX));
      segmentNameGeneratorSpec.addConfig(SegmentGenerationTaskRunner.APPEND_UUID_TO_SEGMENT_NAME,
          taskConfigs.getOrDefault(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, Boolean.toString(false)));
      taskSpec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);
      taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

      return taskSpec;
    }
  }
}
