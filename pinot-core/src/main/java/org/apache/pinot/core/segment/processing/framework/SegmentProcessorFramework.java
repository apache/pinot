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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitioningConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A framework to process "m" given segments and convert them into "n" segments
 * The phases of the Segment Processor are
 * 1. Map - record transformation, partitioning, partition filtering
 * 2. Reduce - rollup, concat, split etc
 * 3. Segment generation
 *
 * This will typically be used by minion tasks, which want to perform some processing on segments
 * (eg task which merges segments, tasks which aligns segments per time boundaries etc)
 */
public class SegmentProcessorFramework {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentProcessorFramework.class);

  private final File _inputSegmentsDir;
  private final File _outputSegmentsDir;
  private final SegmentProcessorConfig _segmentProcessorConfig;

  private final Schema _pinotSchema;
  private final TableConfig _tableConfig;

  private final File _baseDir;
  private final File _mapperInputDir;
  private final File _mapperOutputDir;
  private final File _reducerOutputDir;

  /**
   * Initializes the Segment Processor framework with input segments, output path and processing config
   * @param inputSegmentsDir directory containing the input segments. These can be tarred or untarred.
   * @param segmentProcessorConfig config for segment processing
   * @param outputSegmentsDir directory for placing the resulting segments. This should already exist.
   */
  public SegmentProcessorFramework(File inputSegmentsDir, SegmentProcessorConfig segmentProcessorConfig,
      File outputSegmentsDir) {

    LOGGER.info(
        "Initializing SegmentProcessorFramework with input segments dir: {}, output segments dir: {} and segment processor config: {}",
        inputSegmentsDir.getAbsolutePath(), outputSegmentsDir.getAbsolutePath(), segmentProcessorConfig.toString());

    _inputSegmentsDir = inputSegmentsDir;
    Preconditions.checkState(_inputSegmentsDir.exists() && _inputSegmentsDir.isDirectory(),
        "Input path: %s,  must be a directory with Pinot segments", _inputSegmentsDir.getAbsolutePath());

    _outputSegmentsDir = outputSegmentsDir;
    Preconditions.checkState(_outputSegmentsDir.exists() && _outputSegmentsDir.isDirectory(),
        "Must provide valid output directory: %s", _outputSegmentsDir.getAbsolutePath());

    _segmentProcessorConfig = segmentProcessorConfig;
    _pinotSchema = segmentProcessorConfig.getSchema();
    _tableConfig = segmentProcessorConfig.getTableConfig();

    _baseDir = new File(FileUtils.getTempDirectory(), "segment_processor_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    if (!_baseDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + _baseDir + " for SegmentProcessor");
    }
    _mapperInputDir = new File(_baseDir, "mapper_input");
    if (!_mapperInputDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + _mapperInputDir + " for SegmentProcessor");
    }
    _mapperOutputDir = new File(_baseDir, "mapper_output");
    if (!_mapperOutputDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + _mapperOutputDir + " for SegmentProcessor");
    }
    _reducerOutputDir = new File(_baseDir, "reducer_output");
    if (!_reducerOutputDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + _reducerOutputDir + " for SegmentProcessor");
    }
  }

  /**
   * Processes segments from the input directory as per the provided configs, then puts resulting segments into the output directory
   */
  public void processSegments()
      throws Exception {

    // Check for input segments
    File[] segmentFiles = _inputSegmentsDir.listFiles();
    if (segmentFiles.length == 0) {
      throw new IllegalStateException("No segments found in input dir: " + _inputSegmentsDir.getAbsolutePath()
          + ". Exiting SegmentProcessorFramework.");
    }

    // Mapper phase.
    LOGGER.info("Beginning mapper phase. Processing segments: {}", Arrays.toString(_inputSegmentsDir.list()));
    for (File segment : segmentFiles) {

      String fileName = segment.getName();
      File mapperInput = segment;

      // Untar the segments if needed
      if (!segment.isDirectory()) {
        if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
          mapperInput = TarGzCompressionUtils.untar(segment, _mapperInputDir).get(0);
        } else {
          throw new IllegalStateException("Unsupported segment format: " + segment.getAbsolutePath());
        }
      }

      // Set mapperId as the name of the segment
      SegmentMapperConfig mapperConfig = new SegmentMapperConfig(mapperInput.getName(), _pinotSchema,
          _segmentProcessorConfig.getRecordTransformerConfig(), _segmentProcessorConfig.getPartitioningConfig());
      SegmentMapper mapper = new SegmentMapper(mapperInput, mapperConfig, _mapperOutputDir);
      mapper.map();
      mapper.cleanup();
    }

    // Check for mapper output files
    File[] mapperOutputFiles = _mapperOutputDir.listFiles();
    if (mapperOutputFiles.length == 0) {
      throw new IllegalStateException("No files found in mapper output directory: " + _mapperOutputDir.getAbsolutePath()
          + ". Exiting SegmentProcessorFramework.");
    }

    // Reducer phase.
    LOGGER.info("Beginning reducer phase. Processing files: {}", Arrays.toString(_mapperOutputDir.list()));
    // Mapper output directory has 1 directory per partition, named after the partition. Each directory contains 1 or more avro files.
    for (File partDir : mapperOutputFiles) {

      // Set partition as reducerId
      SegmentReducerConfig reducerConfig =
          new SegmentReducerConfig(partDir.getName(), _pinotSchema, _segmentProcessorConfig.getCollectorConfig(),
              _segmentProcessorConfig.getSegmentConfig().getMaxNumRecordsPerSegment());
      SegmentReducer reducer = new SegmentReducer(partDir, reducerConfig, _reducerOutputDir);
      reducer.reduce();
      reducer.cleanup();
    }

    // Check for reducer output files
    File[] reducerOutputFiles = _reducerOutputDir.listFiles();
    if (reducerOutputFiles.length == 0) {
      throw new IllegalStateException(
          "No files found in reducer output directory: " + _reducerOutputDir.getAbsolutePath()
              + ". Exiting SegmentProcessorFramework.");
    }

    // Segment generation phase.
    LOGGER.info("Beginning segment generation phase. Processing files: {}", Arrays.toString(_reducerOutputDir.list()));
    // Reducer output directory will have 1 or more avro files
    for (File resultFile : reducerOutputFiles) {
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _pinotSchema);
      segmentGeneratorConfig.setTableName(_tableConfig.getTableName());
      segmentGeneratorConfig.setOutDir(_outputSegmentsDir.getAbsolutePath());
      segmentGeneratorConfig.setInputFilePath(resultFile.getAbsolutePath());
      segmentGeneratorConfig.setFormat(FileFormat.AVRO);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();
    }

    LOGGER.info("Successfully converted segments from: {} to {}", _inputSegmentsDir,
        Arrays.toString(_outputSegmentsDir.list()));
  }

  /**
   * Cleans up the Segment Processor Framework state
   */
  public void cleanup() {
    FileUtils.deleteQuietly(_baseDir);
  }

  public static void main(String[] args)
      throws IOException {
    TableConfig tableConfig = JsonUtils
        .fileToObject(new File("/Users/npawar/quick_start_configs/segment_processing_framework/offline.json"),
            TableConfig.class);
    Schema schema =
        Schema.fromFile(new File("/Users/npawar/quick_start_configs/segment_processing_framework/schema.json"));
    File inputSegments = new File("/Users/npawar/quick_start_configs/segment_processing_framework/segments");
    File outputSegments = new File("/Users/npawar/quick_start_configs/segment_processing_framework/output");
    FileUtils.deleteQuietly(outputSegments);
    outputSegments.mkdirs();

    Map<String, String> transformFunctionsMap = new HashMap<>();
    transformFunctionsMap.put("timeValue", "round(timeValue, 86400000)");
    RecordTransformerConfig transformerConfig =
        new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionsMap).build();
    PartitioningConfig partitioningConfig =
        new PartitioningConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("timeValue").setFilterFunction("Groovy({arg0 != \"1597795200000\"}, arg0)").build();
    SegmentConfig segmentConfig = new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).build();
    Map<String, ValueAggregatorFactory.ValueAggregatorType> valueAggregatorsMap = new HashMap<>();
    valueAggregatorsMap.put("clicks", ValueAggregatorFactory.ValueAggregatorType.MAX);
    CollectorConfig collectorConfig =
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP)
            .setAggregatorTypeMap(valueAggregatorsMap).build();
    SegmentProcessorConfig segmentProcessorConfig =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
            .setRecordTransformerConfig(transformerConfig).setPartitioningConfig(partitioningConfig)
            .setCollectorConfig(collectorConfig).setSegmentConfig(segmentConfig).build();
    System.out.println(segmentProcessorConfig);

    SegmentProcessorFramework framework = null;
    try {
      framework = new SegmentProcessorFramework(inputSegments, segmentProcessorConfig, outputSegments);
      framework.processSegments();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (framework != null) {
        framework.cleanup();
      }
    }
  }
}
