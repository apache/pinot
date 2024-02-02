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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentGenerationAndPushTaskGenerator generates task configs for SegmentGenerationAndPush minion tasks.
 *
 * This generator consumes configs from org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig:
 *   inputDirURI - Required, the location of input data directory
 *   inputFormat - Required, the input file format, e.g. JSON/Avro/Parquet/CSV/...
 *   input.fs.className - Optional, the class name of filesystem to read input data. Default to be inferred from
 *   inputDirURI if not specified.
 *   input.fs.prop.<keys> - Optional, defines the configs to initialize input filesystem.
 *
 *   outputDirURI - Optional, the location of output segments. Use local temp dir with push mode TAR, If not specified.
 *   output.fs.className - Optional, the class name of filesystem to write output segments. Default to be inferred
 *   from outputDirURI if not specified.
 *   output.fs.prop.<keys> - Optional, the configs to initialize output filesystem.
 *   overwriteOutput - Optional, delete the output segment directory if set to true.
 *
 *   recordReader.className - Optional, the class name of RecordReader. Default to be inferred from inputFormat if
 *   not specified.
 *   recordReader.configClassName - Optional, the class name of RecordReaderConfig. Default to be inferred from
 *   inputFormat if not specified.
 *   recordReader.prop.<keys> - Optional, the configs used to initialize RecordReaderConfig.
 *
 *   schema - Optional, Pinot schema in Json string.
 *   schemaURI - Optional, the URI to query for Pinot schema.
 *
 *   segmentNameGenerator.type - Optional, the segment name generator to create segment name.
 *   segmentNameGenerator.configs.<keys> - Optional, configs of segment name generator.
 *
 *   push.mode - Optional, push job type: TAR/URI/METADATA, default to METADATA
 *   push.controllerUri - Optional, controller uri to send push request to, default to the controller vip uri.
 *   push.segmentUriPrefix - Optional, segment download uri prefix, used when push.mode=uri.
 *   push.segmentUriSuffix - Optional, segment download uri suffix, used when push.mode=uri.
 *
 */
@TaskGenerator
public class SegmentGenerationAndPushTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationAndPushTaskGenerator.class);
  private static final BatchConfigProperties.SegmentPushType DEFAULT_SEGMENT_PUSH_TYPE =
      BatchConfigProperties.SegmentPushType.TAR;

  @Override
  public String getTaskType() {
    return MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String tableNameWithType = tableConfig.getTableName();
      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkNotNull(tableTaskConfig);
      Map<String, String> taskConfigs =
          tableTaskConfig.getConfigsForTaskType(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: %s", tableNameWithType);

      // Get max number of tasks for this table
      int tableMaxNumTasks;
      String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
      if (tableMaxNumTasksConfig != null) {
        try {
          tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
        } catch (NumberFormatException e) {
          tableMaxNumTasks = Integer.MAX_VALUE;
        }
      } else {
        tableMaxNumTasks = Integer.MAX_VALUE;
      }

      // Generate tasks
      int tableNumTasks = 0;
      // Generate up to tableMaxNumTasks tasks each time for each table
      if (tableNumTasks == tableMaxNumTasks) {
        break;
      }
      String batchSegmentIngestionType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      List<Map<String, String>> batchConfigMaps = batchIngestionConfig.getBatchConfigMaps();
      for (Map<String, String> batchConfigMap : batchConfigMaps) {
        try {
          URI inputDirURI =
              SegmentGenerationUtils.getDirectoryURI(batchConfigMap.get(BatchConfigProperties.INPUT_DIR_URI));
          updateRecordReaderConfigs(batchConfigMap);
          List<SegmentZKMetadata> segmentsZKMetadata = Collections.emptyList();
          // For append mode, we don't create segments for input file URIs already created.
          if (BatchConfigProperties.SegmentIngestionType.APPEND.name().equalsIgnoreCase(batchSegmentIngestionType)) {
            segmentsZKMetadata = getSegmentsZKMetadataForTable(tableNameWithType);
          }
          Set<String> existingSegmentInputFiles = getExistingSegmentInputFiles(segmentsZKMetadata);
          Set<String> inputFilesFromRunningTasks = getInputFilesFromRunningTasks(tableNameWithType);
          existingSegmentInputFiles.addAll(inputFilesFromRunningTasks);
          LOGGER.info("Trying to extract input files from path: {}, "
                  + "and exclude input files from existing segments metadata: {}, "
                  + "and input files from running tasks: {}", inputDirURI, existingSegmentInputFiles,
              inputFilesFromRunningTasks);
          List<URI> inputFileURIs = getInputFilesFromDirectory(batchConfigMap, inputDirURI, existingSegmentInputFiles);
          if (inputFileURIs.size() < 10) {
            LOGGER.info("Final input files for task config generation: {}", inputFileURIs);
          } else {
            LOGGER.info("Final input files for task config generation: {}...", inputFileURIs.subList(0, 10));
          }
          for (URI inputFileURI : inputFileURIs) {
            Map<String, String> singleFileGenerationTaskConfig =
                getSingleFileGenerationTaskConfig(tableNameWithType, tableNumTasks, batchConfigMap, inputFileURI, null);
            pinotTaskConfigs.add(new PinotTaskConfig(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
                singleFileGenerationTaskConfig));
            tableNumTasks++;

            // Generate up to tableMaxNumTasks tasks each time for each table
            if (tableNumTasks == tableMaxNumTasks) {
              break;
            }
          }
        } catch (Exception e) {
          LOGGER.error("Unable to generate the SegmentGenerationAndPush task. [ table configs: {}, task configs: {} ]",
              tableConfig, taskConfigs, e);
        }
      }
    }
    return pinotTaskConfigs;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    String taskUUID = UUID.randomUUID().toString();
    String tableNameWithType = tableConfig.getTableName();

    // Override task configs from table with adhoc task configs.
    Map<String, String> batchConfigMap = new HashMap<>();
    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    if (tableTaskConfig != null) {
      batchConfigMap.putAll(
          tableTaskConfig.getConfigsForTaskType(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE));
    }
    batchConfigMap.putAll(taskConfigs);

    int tableNumTasks = 0;
    try {
      URI inputDirURI =
          SegmentGenerationUtils.getDirectoryURI(batchConfigMap.get(BatchConfigProperties.INPUT_DIR_URI));
      List<URI> inputFileURIs = getInputFilesFromDirectory(batchConfigMap, inputDirURI, Collections.emptySet());
      if (inputFileURIs.isEmpty()) {
        LOGGER.warn("Skip generating SegmentGenerationAndPushTask, no input files found : {}", inputDirURI);
        return ImmutableList.of();
      }
      if (!batchConfigMap.containsKey(BatchConfigProperties.INPUT_FORMAT)) {
        batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT,
            extractFormatFromFileSuffix(inputFileURIs.get(0).getPath()));
      }
      updateRecordReaderConfigs(batchConfigMap);

      List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
      LOGGER.info("Final input files for task config generation: {}", inputFileURIs);
      for (URI inputFileURI : inputFileURIs) {
        Map<String, String> singleFileGenerationTaskConfig =
            getSingleFileGenerationTaskConfig(tableNameWithType, tableNumTasks, batchConfigMap, inputFileURI,
                generateFixedSegmentName(tableNameWithType, taskUUID, tableNumTasks));
        pinotTaskConfigs.add(new PinotTaskConfig(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            singleFileGenerationTaskConfig));
        tableNumTasks++;
      }
      if (!batchConfigMap.containsKey(BatchConfigProperties.INPUT_FORMAT)) {
        batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT,
            extractFormatFromFileSuffix(inputFileURIs.get(0).getPath()));
        updateRecordReaderConfigs(batchConfigMap);
      }
      return pinotTaskConfigs;
    } catch (Exception e) {
      LOGGER.error("Unable to generate the SegmentGenerationAndPush task. [ table configs: {}, task configs: {} ]",
          tableConfig, taskConfigs, e);
      throw e;
    }
  }

  private String generateFixedSegmentName(String tableName, String taskUUID, int tableNumTasks) {
    return String.format("%s_%s_%d", tableName, taskUUID, tableNumTasks);
  }

  private String extractFormatFromFileSuffix(String path) {
    int lastDotPosition = path.lastIndexOf(IngestionConfigUtils.DOT_SEPARATOR);
    if (lastDotPosition < 0) {
      throw new UnsupportedOperationException("No file extension found");
    }
    return path.substring(lastDotPosition + 1);
  }

  private Set<String> getInputFilesFromRunningTasks(String tableName) {
    Set<String> inputFilesFromRunningTasks = new HashSet<>();
    TaskGeneratorUtils
        .forRunningTasks(tableName, MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, _clusterInfoAccessor,
            taskConfig -> {
              String inputFileURI = taskConfig.get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY);
              if (inputFileURI != null) {
                inputFilesFromRunningTasks.add(inputFileURI);
              }
            });
    return inputFilesFromRunningTasks;
  }

  private Map<String, String> getSingleFileGenerationTaskConfig(String tableName, int sequenceID,
      Map<String, String> batchConfigMap, URI inputFileURI, @Nullable String segmentName)
      throws URISyntaxException {

    URI inputDirURI = SegmentGenerationUtils.getDirectoryURI(batchConfigMap.get(BatchConfigProperties.INPUT_DIR_URI));
    URI outputDirURI = null;
    if (batchConfigMap.containsKey(BatchConfigProperties.OUTPUT_DIR_URI)) {
      outputDirURI = SegmentGenerationUtils.getDirectoryURI(batchConfigMap.get(BatchConfigProperties.OUTPUT_DIR_URI));
    }
    String pushMode = IngestionConfigUtils.getPushMode(batchConfigMap);

    Map<String, String> singleFileGenerationTaskConfig = new HashMap<>(batchConfigMap);
    singleFileGenerationTaskConfig
        .put(BatchConfigProperties.TABLE_NAME, tableName);
    singleFileGenerationTaskConfig.put(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());
    if (outputDirURI != null) {
      URI outputSegmentDirURI = SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI);
      singleFileGenerationTaskConfig.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputSegmentDirURI.toString());
    }
    singleFileGenerationTaskConfig.put(BatchConfigProperties.SEQUENCE_ID, String.valueOf(sequenceID));
    if (!singleFileGenerationTaskConfig.containsKey(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE)) {
      if (segmentName == null) {
        singleFileGenerationTaskConfig.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
            BatchConfigProperties.SegmentNameGeneratorType.SIMPLE);
      } else {
        singleFileGenerationTaskConfig.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
            BatchConfigProperties.SegmentNameGeneratorType.FIXED);
        singleFileGenerationTaskConfig.put(
            BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + "." + BatchConfigProperties.SEGMENT_NAME,
            segmentName);
      }
    }
    // The SEQUENCE_ID used to identify each segment is only unique to each round of task generation. Across multiple
    // rounds of task generation, the SEQUENCE_ID can be the same.
    // This may lead to segment name collision, and existing segments will be overridden. Since old segments are
    // overridden, corresponding files become un-ingested, the generator will generate new tasks, which generates
    // segments with same names, and override existing segments again... It becomes an endless loop
    // We add uuid to segment name to avoid segment collision across multiple rounds of task generation to solve the
    // problem.
    singleFileGenerationTaskConfig.put(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, Boolean.toString(true));
    if ((outputDirURI == null) || (pushMode == null)) {
      singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE, DEFAULT_SEGMENT_PUSH_TYPE.toString());
    } else {
      singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE, pushMode);
    }
    singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _clusterInfoAccessor.getVipUrl());
    return singleFileGenerationTaskConfig;
  }

  private void updateRecordReaderConfigs(Map<String, String> batchConfigMap) {
    String inputFormat = batchConfigMap.get(BatchConfigProperties.INPUT_FORMAT);
    String recordReaderClassName = PluginManager.get().getRecordReaderClassName(inputFormat);
    if (recordReaderClassName != null) {
      batchConfigMap.putIfAbsent(BatchConfigProperties.RECORD_READER_CLASS, recordReaderClassName);
    }
    String recordReaderConfigClassName = PluginManager.get().getRecordReaderConfigClassName(inputFormat);
    if (recordReaderConfigClassName != null) {
      batchConfigMap.putIfAbsent(BatchConfigProperties.RECORD_READER_CONFIG_CLASS, recordReaderConfigClassName);
    }
  }

  private List<URI> getInputFilesFromDirectory(Map<String, String> batchConfigMap, URI inputDirURI,
      Set<String> existingSegmentInputFileURIs)
      throws Exception {
    try (PinotFS inputDirFS = MinionTaskUtils.getInputPinotFS(batchConfigMap, inputDirURI)) {

      String includeFileNamePattern = batchConfigMap.get(BatchConfigProperties.INCLUDE_FILE_NAME_PATTERN);
      String excludeFileNamePattern = batchConfigMap.get(BatchConfigProperties.EXCLUDE_FILE_NAME_PATTERN);

      //Get list of files to process
      String[] files;
      try {
        files = inputDirFS.listFiles(inputDirURI, true);
      } catch (IOException e) {
        LOGGER.error("Unable to list files under URI: " + inputDirURI, e);
        return Collections.emptyList();
      }
      PathMatcher includeFilePathMatcher = null;
      if (includeFileNamePattern != null) {
        includeFilePathMatcher = FileSystems.getDefault().getPathMatcher(includeFileNamePattern);
      }
      PathMatcher excludeFilePathMatcher = null;
      if (excludeFileNamePattern != null) {
        excludeFilePathMatcher = FileSystems.getDefault().getPathMatcher(excludeFileNamePattern);
      }
      List<URI> inputFileURIs = new ArrayList<>();
      for (String file : files) {
        LOGGER.debug("Processing file: {}", file);
        if (includeFilePathMatcher != null) {
          if (!includeFilePathMatcher.matches(Paths.get(file))) {
            LOGGER.debug("Exclude file {} as it's not matching includeFilePathMatcher: {}",
                file, includeFileNamePattern);
            continue;
          }
        }
        if (excludeFilePathMatcher != null) {
          if (excludeFilePathMatcher.matches(Paths.get(file))) {
            LOGGER.debug("Exclude file {} as it's matching excludeFilePathMatcher: {}", file, excludeFileNamePattern);
            continue;
          }
        }
        try {
          URI inputFileURI = SegmentGenerationUtils.getFileURI(file, inputDirURI);
          if (existingSegmentInputFileURIs.contains(inputFileURI.toString())) {
            LOGGER.debug("Skipping already processed inputFileURI: {}", inputFileURI);
            continue;
          }
          if (inputDirFS.isDirectory(inputFileURI)) {
            LOGGER.debug("Skipping directory: {}", inputFileURI);
            continue;
          }
          inputFileURIs.add(inputFileURI);
        } catch (Exception e) {
          LOGGER.error("Failed to construct inputFileURI for path: {}, parent directory URI: {}", file, inputDirURI, e);
          continue;
        }
      }

      return inputFileURIs;
    }
  }

  private Set<String> getExistingSegmentInputFiles(List<SegmentZKMetadata> segmentsZKMetadata) {
    Set<String> existingSegmentInputFiles = new HashSet<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      Map<String, String> customMap = segmentZKMetadata.getCustomMap();
      if (customMap != null && customMap.containsKey(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY)) {
        existingSegmentInputFiles.add(customMap.get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY));
      }
    }
    return existingSegmentInputFiles;
  }
}
