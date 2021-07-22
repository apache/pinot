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
package org.apache.pinot.ingestion.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pinot.ingestion.preprocess.partitioners.AvroDataPreprocessingPartitioner;
import org.apache.pinot.ingestion.utils.preprocess.HadoopUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroDataPreprocessingHelper extends DataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataPreprocessingHelper.class);

  public AvroDataPreprocessingHelper(List<Path> inputDataPaths, Path outputPath) {
    super(inputDataPaths, outputPath);
  }

  @Override
  public Class<? extends Partitioner> getPartitioner() {
    return AvroDataPreprocessingPartitioner.class;
  }

  @Override
  public Object getSchema(Path inputPathDir)
      throws IOException {
    return getAvroSchema(inputPathDir);
  }

  @Override
  public void validateConfigsAgainstSchema(Object schema) {
    Schema avroSchema = (Schema) schema;
    validateConfigsAgainstSchema(avroSchema);
  }

  @Override
  public String getSampleTimeColumnValue(String timeColumnName)
      throws IOException {
    String sampleTimeColumnValue;
    try (DataFileStream<GenericRecord> dataStreamReader = getAvroReader(_sampleRawDataPath)) {
      sampleTimeColumnValue = dataStreamReader.next().get(timeColumnName).toString();
    }
    return sampleTimeColumnValue;
  }

  /**
   * Finds the avro file in the input folder, and returns its avro schema
   * @param inputPathDir Path to input directory
   * @return Input schema
   * @throws IOException exception when accessing to IO
   */
  protected Schema getAvroSchema(Path inputPathDir)
      throws IOException {
    Schema avroSchema = null;
    for (FileStatus fileStatus : HadoopUtils.DEFAULT_FILE_SYSTEM.listStatus(inputPathDir)) {
      if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".avro")) {
        LOGGER.info("Extracting schema from " + fileStatus.getPath());
        try (DataFileStream<GenericRecord> dataStreamReader = getAvroReader(inputPathDir)) {
          avroSchema = dataStreamReader.getSchema();
        }
        break;
      }
    }
    return avroSchema;
  }

  /**
   * Helper method that returns avro reader for the given avro file.
   * If file name ends in 'gz' then returns the GZIP version, otherwise gives the regular reader.
   *
   * @param avroFile File to read
   * @return Avro reader for the file.
   * @throws IOException exception when accessing to IO
   */
  private DataFileStream<GenericRecord> getAvroReader(Path avroFile)
      throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    if (avroFile.getName().endsWith("gz")) {
      return new DataFileStream<>(new GZIPInputStream(fs.open(avroFile)), new GenericDatumReader<>());
    } else {
      return new DataFileStream<>(fs.open(avroFile), new GenericDatumReader<>());
    }
  }

  private void validateConfigsAgainstSchema(Schema schema) {
    if (_partitionColumn != null) {
      Preconditions.checkArgument(schema.getField(_partitionColumn) != null,
          String.format("Partition column: %s is not found from the schema of input files.", _partitionColumn));
      Preconditions.checkArgument(_numPartitions > 0,
          String.format("Number of partitions should be positive. Current value: %s", _numPartitions));
      Preconditions.checkArgument(_partitionFunction != null, "Partition function should not be null!");
      try {
        PartitionFunctionFactory.PartitionFunctionType.fromString(_partitionFunction);
      } catch (IllegalArgumentException e) {
        LOGGER.error("Partition function needs to be one of Modulo, Murmur, ByteArray, HashCode, it is currently {}",
            _partitionColumn);
        throw new IllegalArgumentException(e);
      }
    }
    if (_sortingColumn != null) {
      Preconditions.checkArgument(schema.getField(_sortingColumn) != null,
          String.format("Sorted column: %s is not found from the schema of input files.", _sortingColumn));
    }
  }
}
