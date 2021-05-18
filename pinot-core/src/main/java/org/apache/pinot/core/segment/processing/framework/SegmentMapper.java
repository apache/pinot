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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.processing.filter.RecordFilter;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerFactory;
import org.apache.pinot.core.util.SegmentProcessorAvroUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.partition.Partitioner;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mapper phase of the SegmentProcessorFramework.
 * Reads the input segment and creates partitioned avro data files
 * Performs:
 * - record filtering
 * - column transformations
 * - partitioning
 */
public class SegmentMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMapper.class);
  private final File _inputSegment;
  private final File _mapperOutputDir;

  private final String _mapperId;
  private final Schema _avroSchema;
  private final RecordTransformer _recordTransformer;
  private final RecordFilter _recordFilter;
  private final int _numPartitioners;
  private final List<Partitioner> _partitioners = new ArrayList<>();
  private final Map<String, DataFileWriter<GenericData.Record>> _partitionToDataFileWriterMap = new HashMap<>();

  public SegmentMapper(String mapperId, File inputSegment, SegmentMapperConfig mapperConfig, File mapperOutputDir) {
    _inputSegment = inputSegment;
    _mapperOutputDir = mapperOutputDir;

    _mapperId = mapperId;
    _avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(mapperConfig.getPinotSchema());
    _recordFilter = RecordFilterFactory.getRecordFilter(mapperConfig.getRecordFilterConfig());
    _recordTransformer = RecordTransformerFactory.getRecordTransformer(mapperConfig.getRecordTransformerConfig());
    for (PartitionerConfig partitionerConfig : mapperConfig.getPartitionerConfigs()) {
      _partitioners.add(PartitionerFactory.getPartitioner(partitionerConfig));
    }
    _numPartitioners = _partitioners.size();
    LOGGER.info(
        "Initialized mapper with id: {}, input segment: {}, output dir: {}, recordTransformer: {}, recordFilter: {}, partitioners: {}",
        _mapperId, _inputSegment, _mapperOutputDir, _recordTransformer.getClass(), _recordFilter.getClass(),
        _partitioners.stream().map(p -> p.getClass().toString()).collect(Collectors.joining(",")));
  }

  /**
   * Reads the input segment and generates partitioned avro data files into the mapper output directory
   * Records for each partition are put into a directory of its own withing the mapper output directory, identified by the partition name
   */
  public void map()
      throws Exception {

    PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_inputSegment);
    GenericRow reusableRow = new GenericRow();
    GenericData.Record reusableRecord = new GenericData.Record(_avroSchema);
    String[] partitions = new String[_numPartitioners];

    while (segmentRecordReader.hasNext()) {
      reusableRow = segmentRecordReader.next(reusableRow);

      // Record filtering
      if (_recordFilter.filter(reusableRow)) {
        continue;
      }

      // Record transformation
      reusableRow = _recordTransformer.transformRecord(reusableRow);

      // Partitioning
      int p = 0;
      for (Partitioner partitioner : _partitioners) {
        partitions[p++] = partitioner.getPartition(reusableRow);
      }
      String partition = StringUtil.join("_", partitions);

      // Create writer for the partition, if not exists
      DataFileWriter<GenericData.Record> recordWriter = _partitionToDataFileWriterMap.get(partition);
      if (recordWriter == null) {
        File partDir = new File(_mapperOutputDir, partition);
        if (!partDir.exists()) {
          Files.createDirectory(Paths.get(partDir.getAbsolutePath()));
        }
        recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
        recordWriter.create(_avroSchema, new File(partDir, createMapperOutputFileName(_mapperId)));
        _partitionToDataFileWriterMap.put(partition, recordWriter);
      }

      // Write record to avro file for its partition
      SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(reusableRow, reusableRecord);
      recordWriter.append(reusableRecord);

      reusableRow.clear();
    }
  }

  /**
   * Cleanup the mapper state
   */
  public void cleanup()
      throws IOException {
    for (DataFileWriter<GenericData.Record> recordDataFileWriter : _partitionToDataFileWriterMap.values()) {
      recordDataFileWriter.close();
    }
  }

  public static String createMapperOutputFileName(String mapperId) {
    return "mapper_" + mapperId + ".avro";
  }
}
