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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.core.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.JsonUtils;


public class SegmentGenerationTaskRunner implements Serializable {

  public static final String SIMPLE_SEGMENT_NAME_GENERATOR = "simple";
  public static final String NORMALIZED_DATE_SEGMENT_NAME_GENERATOR = "normalizedDate";

  // For SimpleSegmentNameGenerator
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  // For NormalizedDateSegmentNameGenerator
  public static final String SEGMENT_NAME_PREFIX = "segment.name.prefix";
  public static final String EXCLUDE_SEQUENCE_ID = "exclude.sequence.id";

  private SegmentGenerationTaskSpec _taskSpec;

  public SegmentGenerationTaskRunner(SegmentGenerationTaskSpec taskSpec) {
    _taskSpec = taskSpec;
  }

  public String run()
      throws Exception {
    TableConfig tableConfig = JsonUtils.jsonNodeToObject(_taskSpec.getTableConfig(), TableConfig.class);
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
      JsonNode jsonNode = new ObjectMapper().valueToTree(configs);
      Class<?> clazz = PluginManager.get().loadClass(readerConfigClassName);
      recordReaderConfig = (RecordReaderConfig) JsonUtils.jsonNodeToObject(jsonNode, clazz);
    }

    //init record reader
    String readerClassName = _taskSpec.getRecordReaderSpec().getClassName();
    RecordReader recordReader = PluginManager.get().createInstance(readerClassName);

    recordReader.init(new File(_taskSpec.getInputFilePath()), schema, recordReaderConfig);

    //init segmentName Generator
    SegmentNameGenerator segmentNameGenerator = getSegmentNameGerator();

    //init segment generation config
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(_taskSpec.getOutputDirectoryPath());
    segmentGeneratorConfig.setSegmentNameGenerator(segmentNameGenerator);
    segmentGeneratorConfig.setSequenceId(_taskSpec.getSequenceId());

    //build segment
    SegmentIndexCreationDriverImpl segmentIndexCreationDriver = new SegmentIndexCreationDriverImpl();
    segmentIndexCreationDriver.init(segmentGeneratorConfig, recordReader);
    segmentIndexCreationDriver.build();
    return segmentIndexCreationDriver.getSegmentName();
  }

  private SegmentNameGenerator getSegmentNameGerator()
      throws IOException {
    TableConfig tableConfig = JsonUtils.jsonNodeToObject(_taskSpec.getTableConfig(), TableConfig.class);
    String tableName = tableConfig.getTableName();

    Schema schema = _taskSpec.getSchema();
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = _taskSpec.getSegmentNameGeneratorSpec();
    if (segmentNameGeneratorSpec == null) {
      segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    }
    String segmentNameGeneratorType = segmentNameGeneratorSpec.getType();
    if (segmentNameGeneratorType == null) {
      segmentNameGeneratorType = SIMPLE_SEGMENT_NAME_GENERATOR;
    }
    Map<String, String> segmentNameGeneratorConfigs = segmentNameGeneratorSpec.getConfigs();
    if (segmentNameGeneratorConfigs == null) {
      segmentNameGeneratorConfigs = new HashMap<>();
    }
    switch (segmentNameGeneratorType) {
      case SIMPLE_SEGMENT_NAME_GENERATOR:
        return new SimpleSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_POSTFIX));
      case NORMALIZED_DATE_SEGMENT_NAME_GENERATOR:
        Preconditions.checkState(tableConfig != null,
            "In order to use NormalizedDateSegmentNameGenerator, table config must be provided");
        SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
        String timeFormat = null;
        TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
        if (timeFieldSpec != null) {
          timeFormat = timeFieldSpec.getOutgoingGranularitySpec().getTimeFormat();
        }
        return new NormalizedDateSegmentNameGenerator(tableName, segmentNameGeneratorConfigs.get(SEGMENT_NAME_PREFIX),
            Boolean.valueOf(segmentNameGeneratorConfigs.get(EXCLUDE_SEQUENCE_ID)),
            validationConfig.getSegmentPushType(), validationConfig.getSegmentPushFrequency(),
            validationConfig.getTimeType(), timeFormat);
      default:
        throw new UnsupportedOperationException("Unsupported segment name generator type: " + segmentNameGeneratorType);
    }
  }
}
