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
package org.apache.pinot.controller.helix.core.minion.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.io.File;
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
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentGenerationAndPushTaskGenerator generates task configs for SegmentGenerationAndPush minion tasks.
 *
 * This generator consumes configs from org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig:
 *   inputDirURI - Required, the location of input data directory
 *   inputFormat - Required, the input file format, e.g. JSON/Avro/Parquet/CSV/...
 *   input.fs.className - Optional, the class name of filesystem to read input data. Default to be inferred from inputDirURI if not specified.
 *   input.fs.prop.<keys> - Optional, defines the configs to initialize input filesystem.
 *
 *   outputDirURI - Optional, the location of output segments. Use local temp dir with push mode TAR, If not specified.
 *   output.fs.className - Optional, the class name of filesystem to write output segments. Default to be inferred from outputDirURI if not specified.
 *   output.fs.prop.<keys> - Optional, the configs to initialize output filesystem.
 *   overwriteOutput - Optional, delete the output segment directory if set to true.
 *
 *   recordReader.className - Optional, the class name of RecordReader. Default to be inferred from inputFormat if not specified.
 *   recordReader.configClassName - Optional, the class name of RecordReaderConfig. Default to be inferred from inputFormat if not specified.
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
public class SegmentGenerationAndPushTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationAndPushTaskGenerator.class);
  private static final BatchConfigProperties.SegmentPushType DEFAULT_SEGMENT_PUSH_TYPE =
      BatchConfigProperties.SegmentPushType.TAR;

  private ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public String getTaskType() {
    return MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      // Only generate tasks for OFFLINE tables
      String offlineTableName = tableConfig.getTableName();
      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating SegmentGenerationAndPushTask for non-OFFLINE table: {}", offlineTableName);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkNotNull(tableTaskConfig);
      Map<String, String> taskConfigs =
          tableTaskConfig.getConfigsForTaskType(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", offlineTableName);

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
      String batchSegmentIngestionFrequency = IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig);
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      List<Map<String, String>> batchConfigMaps = batchIngestionConfig.getBatchConfigMaps();
      for (Map<String, String> batchConfigMap : batchConfigMaps) {
        try {
          URI inputDirURI = getDirectoryUri(batchConfigMap.get(BatchConfigProperties.INPUT_DIR_URI));
          updateRecordReaderConfigs(batchConfigMap);
          List<OfflineSegmentZKMetadata> offlineSegmentsMetadata = Collections.emptyList();
          // For append mode, we don't create segments for input file URIs already created.
          if (BatchConfigProperties.SegmentIngestionType.APPEND.name().equalsIgnoreCase(batchSegmentIngestionType)) {
            offlineSegmentsMetadata = this._clusterInfoAccessor.getOfflineSegmentsMetadata(offlineTableName);
          }
          Set<String> existingSegmentInputFiles = getExistingSegmentInputFiles(offlineSegmentsMetadata);
          existingSegmentInputFiles.addAll(getInputFilesFromRunningTasks());
          List<URI> inputFileURIs = getInputFilesFromDirectory(batchConfigMap, inputDirURI, existingSegmentInputFiles);

          for (URI inputFileURI : inputFileURIs) {
            Map<String, String> singleFileGenerationTaskConfig =
                getSingleFileGenerationTaskConfig(offlineTableName, tableNumTasks, batchConfigMap, inputFileURI);
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

  private Set<String> getInputFilesFromRunningTasks() {
    Set<String> inputFilesFromRunningTasks = new HashSet<>();
    Map<String, TaskState> taskStates =
        _clusterInfoAccessor.getTaskStates(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
    for (String taskName : taskStates.keySet()) {
      switch (taskStates.get(taskName)) {
        case FAILED:
        case ABORTED:
        case STOPPED:
        case COMPLETED:
          continue;
      }
      List<PinotTaskConfig> taskConfigs = _clusterInfoAccessor.getTaskConfigs(taskName);
      for (PinotTaskConfig taskConfig : taskConfigs) {
        if (MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE.equalsIgnoreCase(taskConfig.getTaskType())) {
          String inputFileURI = taskConfig.getConfigs().get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY);
          if (inputFileURI != null) {
            inputFilesFromRunningTasks.add(inputFileURI);
          }
        }
      }
    }
    return inputFilesFromRunningTasks;
  }

  private Map<String, String> getSingleFileGenerationTaskConfig(String offlineTableName, int sequenceID,
      Map<String, String> batchConfigMap, URI inputFileURI)
      throws JsonProcessingException, URISyntaxException {

    URI inputDirURI = getDirectoryUri(batchConfigMap.get(BatchConfigProperties.INPUT_DIR_URI));
    URI outputDirURI = null;
    if (batchConfigMap.containsKey(BatchConfigProperties.OUTPUT_DIR_URI)) {
      outputDirURI = getDirectoryUri(batchConfigMap.get(BatchConfigProperties.OUTPUT_DIR_URI));
    }
    String pushMode = IngestionConfigUtils.getPushMode(batchConfigMap);

    Map<String, String> singleFileGenerationTaskConfig = new HashMap<>(batchConfigMap);
    singleFileGenerationTaskConfig.put(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());
    if (outputDirURI != null) {
      URI outputSegmentDirURI = getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI);
      singleFileGenerationTaskConfig.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputSegmentDirURI.toString());
    }
    singleFileGenerationTaskConfig.put(BatchConfigProperties.SCHEMA,
        JsonUtils.objectToString(_clusterInfoAccessor.getTableSchema(offlineTableName)));
    singleFileGenerationTaskConfig.put(BatchConfigProperties.TABLE_CONFIGS,
        JsonUtils.objectToString(_clusterInfoAccessor.getTableConfig(offlineTableName)));
    singleFileGenerationTaskConfig.put(BatchConfigProperties.SEQUENCE_ID, String.valueOf(sequenceID));
    singleFileGenerationTaskConfig
        .put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE, BatchConfigProperties.SegmentNameGeneratorType.SIMPLE);
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
      Set<String> existingSegmentInputFileURIs) {
    String inputDirURIScheme = inputDirURI.getScheme();
    if (!PinotFSFactory.isSchemeSupported(inputDirURIScheme)) {
      String fsClass = batchConfigMap.get(BatchConfigProperties.INPUT_FS_CLASS);
      PinotConfiguration fsProps = IngestionConfigUtils.getInputFsProps(batchConfigMap);
      PinotFSFactory.register(inputDirURIScheme, fsClass, fsProps);
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURIScheme);

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
      try {
        URI inputFileURI = getFileURI(file, inputDirURI);
        if (inputDirFS.isDirectory(inputFileURI) || existingSegmentInputFileURIs.contains(inputFileURI.toString())) {
          continue;
        }
        inputFileURIs.add(inputFileURI);
      } catch (Exception e) {
        continue;
      }
    }
    return inputFileURIs;
  }

  private URI getFileURI(String uriStr, URI fullUriForPathOnlyUriStr)
      throws URISyntaxException {
    URI fileURI = URI.create(uriStr);
    if (fileURI.getScheme() == null) {
      return new URI(fullUriForPathOnlyUriStr.getScheme(), fullUriForPathOnlyUriStr.getAuthority(), fileURI.getPath(),
          fileURI.getQuery(), fileURI.getFragment());
    }
    return fileURI;
  }

  private Set<String> getExistingSegmentInputFiles(List<OfflineSegmentZKMetadata> offlineSegmentsMetadata) {
    Set<String> existingSegmentInputFiles = new HashSet<>();
    for (OfflineSegmentZKMetadata metadata : offlineSegmentsMetadata) {
      if ((metadata.getCustomMap() != null) && metadata.getCustomMap()
          .containsKey(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY)) {
        existingSegmentInputFiles.add(metadata.getCustomMap().get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY));
      }
    }
    return existingSegmentInputFiles;
  }

  private URI getDirectoryUri(String uriStr)
      throws URISyntaxException {
    URI uri = new URI(uriStr);
    if (uri.getScheme() == null) {
      uri = new File(uriStr).toURI();
    }
    return uri;
  }

  private static URI getRelativeOutputPath(URI baseInputDir, URI inputFile, URI outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path based on base input path: " + baseInputDir);
    String outputDirStr = outputDir.toString();
    outputDir = !outputDirStr.endsWith("/") ? URI.create(outputDirStr.concat("/")) : outputDir;
    URI relativeOutputURI = outputDir.resolve(relativePath).resolve(".");
    return relativeOutputURI;
  }
}
