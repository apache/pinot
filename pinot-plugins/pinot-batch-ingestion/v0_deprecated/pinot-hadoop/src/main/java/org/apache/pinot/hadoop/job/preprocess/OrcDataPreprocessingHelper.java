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
package org.apache.pinot.hadoop.job.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.pinot.hadoop.job.mappers.OrcDataPreprocessingMapper;
import org.apache.pinot.hadoop.job.partitioners.OrcDataPreprocessingPartitioner;
import org.apache.pinot.hadoop.job.reducers.OrcDataPreprocessingReducer;
import org.apache.pinot.hadoop.utils.preprocess.HadoopUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class OrcDataPreprocessingHelper extends DataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(OrcDataPreprocessingHelper.class);

  public OrcDataPreprocessingHelper(List<Path> inputDataPaths, Path outputPath) {
    super(inputDataPaths, outputPath);
  }

  @Override
  Class<? extends Partitioner> getPartitioner() {
    return OrcDataPreprocessingPartitioner.class;
  }

  @Override
  void setUpMapperReducerConfigs(Job job) {
    TypeDescription orcSchema = getOrcSchema(_sampleRawDataPath);
    String orcSchemaString = orcSchema.toString();
    LOGGER.info("Orc schema is: {}", orcSchemaString);
    validateConfigsAgainstSchema(orcSchema);

    job.setInputFormatClass(OrcInputFormat.class);
    job.setMapperClass(OrcDataPreprocessingMapper.class);
    job.setMapOutputValueClass(OrcValue.class);
    Configuration jobConf = job.getConfiguration();
    OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(jobConf, orcSchemaString);

    job.setReducerClass(OrcDataPreprocessingReducer.class);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, OrcOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcStruct.class);
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(jobConf, orcSchemaString);
  }

  @Override
  String getSampleTimeColumnValue(String timeColumnName)
      throws IOException {
    try (Reader reader = OrcFile
        .createReader(_sampleRawDataPath, OrcFile.readerOptions(HadoopUtils.DEFAULT_CONFIGURATION))) {
      Reader.Options options = new Reader.Options();
      options.range(0, 1);
      RecordReader records = reader.rows(options);
      TypeDescription orcSchema = reader.getSchema();
      VectorizedRowBatch vectorizedRowBatch = orcSchema.createRowBatch();

      if (records.nextBatch(vectorizedRowBatch)) {
        List<String> orcFields = orcSchema.getFieldNames();
        int numFields = orcFields.size();
        for (int i = 0; i < numFields; i++) {
          String fieldName = orcFields.get(i);
          if (timeColumnName.equals(fieldName)) {
            ColumnVector columnVector = vectorizedRowBatch.cols[i];
            TypeDescription fieldType = orcSchema.getChildren().get(i);
            TypeDescription.Category category = fieldType.getCategory();
            return getValue(fieldName, columnVector, category);
          }
        }
      }
    }
    return null;
  }

  private String getValue(String field, ColumnVector columnVector, TypeDescription.Category category) {
    switch (category) {
      case BOOLEAN:
        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[0]) {
          return Boolean.toString(longColumnVector.vector[0] == 1);
        } else {
          return null;
        }
      case BYTE:
      case SHORT:
      case INT:
        // Extract to Integer
        longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[0]) {
          return Integer.toString((int) longColumnVector.vector[0]);
        } else {
          return null;
        }
      case LONG:
      case DATE:
        // Extract to Long
        longColumnVector = (LongColumnVector) columnVector;
        if (longColumnVector.noNulls || !longColumnVector.isNull[0]) {
          return Long.toString(longColumnVector.vector[0]);
        } else {
          return null;
        }
      case TIMESTAMP:
        // Extract to Long
        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
        if (timestampColumnVector.noNulls || !timestampColumnVector.isNull[0]) {
          return Long.toString(timestampColumnVector.time[0]);
        } else {
          return null;
        }
      case FLOAT:
        // Extract to Float
        DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
        if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[0]) {
          return Float.toString((float) doubleColumnVector.vector[0]);
        } else {
          return null;
        }
      case DOUBLE:
        // Extract to Double
        doubleColumnVector = (DoubleColumnVector) columnVector;
        if (doubleColumnVector.noNulls || !doubleColumnVector.isNull[0]) {
          return Double.toString(doubleColumnVector.vector[0]);
        } else {
          return null;
        }
      case STRING:
      case VARCHAR:
      case CHAR:
        // Extract to String
        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
        if (bytesColumnVector.noNulls || !bytesColumnVector.isNull[0]) {
          int length = bytesColumnVector.length[0];
          return new String(bytesColumnVector.vector[0], bytesColumnVector.start[0], length, UTF_8);
        } else {
          return null;
        }
      case BINARY:
        // Extract to byte[]
        bytesColumnVector = (BytesColumnVector) columnVector;
        if (bytesColumnVector.noNulls || !bytesColumnVector.isNull[0]) {
          int length = bytesColumnVector.length[0];
          byte[] bytes = new byte[length];
          System.arraycopy(bytesColumnVector.vector[0], bytesColumnVector.start[0], bytes, 0, length);
          return new String(bytes, StandardCharsets.UTF_8);
        } else {
          return null;
        }
      default:
        // Unsupported types
        throw new IllegalStateException("Unsupported field type: " + category + " for field: " + field);
    }
  }

  /**
   * Finds the orc file and return its orc schema.
   */
  private TypeDescription getOrcSchema(Path orcFile) {
    TypeDescription orcSchema;
    try (Reader reader = OrcFile.createReader(orcFile, OrcFile.readerOptions(HadoopUtils.DEFAULT_CONFIGURATION))) {
      orcSchema = reader.getSchema();
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while extracting ORC schema from file: " + orcFile, e);
    }
    return orcSchema;
  }

  private void validateConfigsAgainstSchema(TypeDescription schema) {
    List<String> fieldNames = schema.getFieldNames();
    if (_partitionColumn != null) {
      Preconditions.checkArgument(fieldNames.contains(_partitionColumn),
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
      Preconditions
          .checkArgument(fieldNames.contains(_sortingColumn),
              String.format("Sorted column: %s is not found from the schema of input files.", _sortingColumn));
    }
  }
}
