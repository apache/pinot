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
package org.apache.pinot.plugin.ingestion.batch.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.InputFileSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;


@SuppressWarnings("serial")
public class SegmentGenerationTaskRunner implements Serializable {

  // For FixedSegmentNameGenerator
  public static final String SEGMENT_NAME = "segment.name";

  // For SimpleSegmentNameGenerator
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  // For NormalizedDateSegmentNameGenerator
  public static final String SEGMENT_NAME_PREFIX = "segment.name.prefix";
  public static final String EXCLUDE_SEQUENCE_ID = "exclude.sequence.id";

  // For InputFileSegmentNameGenerator
  public static final String FILE_PATH_PATTERN = "file.path.pattern";
  public static final String SEGMENT_NAME_TEMPLATE = "segment.name.template";

  // Assign sequence ids to input files based at each local directory level
  @Deprecated
  public static final String DEPRECATED_USE_LOCAL_DIRECTORY_SEQUENCE_ID = "local.directory.sequence.id";
  public static final String USE_GLOBAL_DIRECTORY_SEQUENCE_ID = "use.global.directory.sequence.id";
  public static final String APPEND_UUID_TO_SEGMENT_NAME = "append.uuid.to.segment.name";
  public static final String OMIT_TIMESTAMPS_IN_SEGMENT_NAME = "omit.timestamps.in.segment.name";

  private final SegmentGenerationTaskSpec _taskSpec;

  public SegmentGenerationTaskRunner(SegmentGenerationTaskSpec taskSpec) {
    _taskSpec = taskSpec;
  }

  public String run()
      throws Exception {
    TableConfig tableConfig = _taskSpec.getTableConfig();
    String tableName = tableConfig.getTableName();
    Schema schema = _taskSpec.getSchema();

    //init record reader config
    String readerConfigClassName = _taskSpec.getRecordReaderSpec().getConfigClassName();
    RecordReaderConfig recordReaderConfig = null;

    if (readerConfigClassName != null) {
      Map<String, String> configs = _taskSpec.getRecordReaderSpec().getConfigs();
      if (configs == null) {
        configs = new HashMap<>();
      }
      JsonNode jsonNode = JsonUtils.objectToJsonNode(configs);
      Class<?> clazz = PluginManager.get().loadClass(readerConfigClassName);
      recordReaderConfig = (RecordReaderConfig) JsonUtils.jsonNodeToObject(jsonNode, clazz);
    }

    //init segment generation config
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(_taskSpec.getOutputDirectoryPath());
    segmentGeneratorConfig.setSequenceId(_taskSpec.getSequenceId());
    segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
    segmentGeneratorConfig.setRecordReaderPath(_taskSpec.getRecordReaderSpec().getClassName());
    segmentGeneratorConfig.setInputFilePath(_taskSpec.getInputFilePath());
    segmentGeneratorConfig.setCustomProperties(_taskSpec.getCustomProperties());
    segmentGeneratorConfig.setFailOnEmptySegment(_taskSpec.isFailOnEmptySegment());

    //init segmentName Generator
    SegmentNameGenerator segmentNameGenerator = getSegmentNameGenerator(segmentGeneratorConfig);
    segmentGeneratorConfig.setSegmentNameGenerator(segmentNameGenerator);

    //build segment
    SegmentIndexCreationDriverImpl segmentIndexCreationDriver = new SegmentIndexCreationDriverImpl();
    segmentIndexCreationDriver.init(segmentGeneratorConfig);
    segmentIndexCreationDriver.build();
    return segmentIndexCreationDriver.getSegmentName();
  }

  private SegmentNameGenerator getSegmentNameGenerator(SegmentGeneratorConfig segmentGeneratorConfig) {
    TableConfig tableConfig = _taskSpec.getTableConfig();
    String tableName = tableConfig.getTableName();

    Schema schema = _taskSpec.getSchema();
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = _taskSpec.getSegmentNameGeneratorSpec();
    if (segmentNameGeneratorSpec == null) {
      segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    }
    String segmentNameGeneratorType = segmentNameGeneratorSpec.getType();
    if (segmentNameGeneratorType == null) {
      segmentNameGeneratorType = segmentGeneratorConfig.inferSegmentNameGeneratorType();
    }
    Map<String, String> segmentNameGeneratorConfigs = segmentNameGeneratorSpec.getConfigs();
    if (segmentNameGeneratorConfigs == null) {
      segmentNameGeneratorConfigs = new HashMap<>();
    }

    boolean appendUUIDToSegmentName =
        Boolean.parseBoolean(segmentNameGeneratorConfigs.get(APPEND_UUID_TO_SEGMENT_NAME));
    boolean omitTimestampsInSegmentName =
        Boolean.parseBoolean(segmentNameGeneratorConfigs.get(OMIT_TIMESTAMPS_IN_SEGMENT_NAME));

    switch (segmentNameGeneratorType) {
      case BatchConfigProperties.SegmentNameGeneratorType.FIXED:
        return new FixedSegmentNameGenerator(segmentNameGeneratorConfigs.get(SEGMENT_NAME));
      case BatchConfigProperties.SegmentNameGeneratorType.SIMPLE:
        return new SimpleSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_POSTFIX),
            appendUUIDToSegmentName, omitTimestampsInSegmentName);
      case BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE:
        SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
        DateTimeFormatSpec dateTimeFormatSpec = null;
        String timeColumnName = validationConfig.getTimeColumnName();

        if (timeColumnName != null) {
          DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
          if (dateTimeFieldSpec != null) {
            dateTimeFormatSpec = dateTimeFieldSpec.getFormatSpec();
          }
        }
        return new NormalizedDateSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_PREFIX),
            Boolean.parseBoolean(segmentNameGeneratorConfigs.get(EXCLUDE_SEQUENCE_ID)),
            IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig),
            IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig), dateTimeFormatSpec,
            segmentNameGeneratorConfigs.get(SEGMENT_NAME_POSTFIX),
            appendUUIDToSegmentName);
      case BatchConfigProperties.SegmentNameGeneratorType.INPUT_FILE:
        String inputFileUri = _taskSpec.getCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY);
        return new InputFileSegmentNameGenerator(segmentNameGeneratorConfigs.get(FILE_PATH_PATTERN),
            segmentNameGeneratorConfigs.get(SEGMENT_NAME_TEMPLATE),
            inputFileUri,
            appendUUIDToSegmentName);
      default:
        throw new UnsupportedOperationException("Unsupported segment name generator type: " + segmentNameGeneratorType);
    }
  }
}
