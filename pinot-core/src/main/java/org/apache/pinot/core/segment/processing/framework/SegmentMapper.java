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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.segment.processing.partitioner.PartitionFilter;
import org.apache.pinot.core.segment.processing.partitioner.Partitioner;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerFactory;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mapper phase of the SegmentProcessorFramework.
 * Reads the input segment and creates partitioned avro data files
 * Performs:
 * - record transformations
 * - partitioning
 * - partition filtering
 */
public class SegmentMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMapper.class);
  private final File _inputSegment;
  private final File _mapperOutputDir;

  private final String _mapperId;
  private final Schema _avroSchema;
  private final RecordTransformer _recordTransformer;
  private final Partitioner _partitioner;
  private final PartitionFilter _partitionFilter;
  private final Map<String, DataFileWriter<GenericData.Record>> _partitionToDataFileWriterMap = new HashMap<>();

  public SegmentMapper(File inputSegment, SegmentMapperConfig mapperConfig, File mapperOutputDir) {
    _inputSegment = inputSegment;
    _mapperOutputDir = mapperOutputDir;

    _mapperId = mapperConfig.getMapperId();
    _avroSchema = SegmentProcessorUtils.convertPinotSchemaToAvroSchema(mapperConfig.getPinotSchema());
    _recordTransformer = RecordTransformerFactory.getRecordTransformer(mapperConfig.getRecordTransformerConfig());
    _partitioner = PartitionerFactory.getPartitioner(mapperConfig.getPartitioningConfig());
    _partitionFilter = PartitionerFactory.getPartitionFilter(mapperConfig.getPartitioningConfig());
    LOGGER.info(
        "Initialized mapper with id: {}, input segment: {}, output dir: {}, recordTransformer: {}, partitioner: {}, partitionFilter: {}",
        _mapperId, _inputSegment, _mapperOutputDir, _recordTransformer.getClass(), _partitioner.getClass(),
        _partitionFilter.getClass());
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

    Set<String> selectedPartitions = new HashSet<>();
    Set<String> rejectedPartitions = new HashSet<>();

    while (segmentRecordReader.hasNext()) {
      reusableRow = segmentRecordReader.next(reusableRow);

      // Record transformation
      reusableRow = _recordTransformer.transformRecord(reusableRow);

      // Partitioning
      String partition = _partitioner.getPartition(reusableRow);

      // Partition filtering
      if (rejectedPartitions.contains(partition)) {
        continue;
      }
      if (!selectedPartitions.contains(partition)) {
        boolean filter = _partitionFilter.filter(partition);
        if (filter) {
          rejectedPartitions.add(partition);
          continue;
        } else {
          selectedPartitions.add(partition);
        }
      }

      // Create writer for the partition, if not exists
      if (!_partitionToDataFileWriterMap.containsKey(partition)) {
        File partDir = new File(_mapperOutputDir, partition);
        if (!partDir.exists()) {
          Files.createDirectory(Paths.get(partDir.getAbsolutePath()));
        }
        DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
        recordWriter.create(_avroSchema, new File(partDir, createMapperOutputFileName(_mapperId)));
        _partitionToDataFileWriterMap.put(partition, recordWriter);
      }

      // Write record to avro file for its partition
      SegmentProcessorUtils.convertGenericRowToAvroRecord(reusableRow, reusableRecord);
      _partitionToDataFileWriterMap.get(partition).append(reusableRecord);
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
