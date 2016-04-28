/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;

/**
 * This class contains methods to extract avro schema, and get
 * avro reader from the avro files
 */
public class ThirdeyeAvroUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(ThirdeyeAvroUtils.class);
  /**
   * extracts avro schema from avro file
   * @param avroFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static Schema extractSchemaFromAvro(Path avroFile) throws IOException {
    DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile);
    Schema avroSchema = dataStreamReader.getSchema();
    dataStreamReader.close();
    return avroSchema;
  }

  private static DataFileStream<GenericRecord> getAvroReader(Path avroFile) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    if(avroFile.getName().endsWith("gz")) {
      return new DataFileStream<GenericRecord>(new GZIPInputStream(fs.open(avroFile)), new GenericDatumReader<GenericRecord>());
    } else {
      return new DataFileStream<GenericRecord>(fs.open(avroFile), new GenericDatumReader<GenericRecord>());
    }
  }

  /**
   * Extracts the datatype of a field from the avro schema, given the name of the field
   * @param fieldname
   * @param schema
   * @return
   */
  public static String getDataTypeForField(String fieldname, Schema schema) {
    Field field = schema.getField(fieldname);
    if (field == null) {
      throw new IllegalStateException("Field " + fieldname + " does not exist in schema");
    }
    return AvroRecordReader.getColumnType(field).toString();
  }

  /**
   * Finds the avro file in the input folder, and returns its avro schema
   * @param inputPathDir
   * @return
   * @throws IOException
   */
  public static Schema getSchema(String inputPathDir) throws IOException  {
    FileSystem fs = FileSystem.get(new Configuration());
    Schema avroSchema = null;
    for (String input : inputPathDir.split(ThirdEyeConstants.FIELD_SEPARATOR)) {
      Path inputPath = new Path(input);
      for (FileStatus fileStatus : fs.listStatus(inputPath)) {
        if (fileStatus.isFile()) {
          avroSchema = extractSchemaFromAvro(fileStatus.getPath());
          break;
        }
      }
    }
    return avroSchema;
  }

  /**
   * Constructs metricTypes property string from the metric names with the help of the avro schema
   * @param metricNamesProperty
   * @param avroSchema
   * @return
   */
  public static String getMetricTypesProperty(String metricNamesProperty, String metricTypesProperty, Schema avroSchema) {
    List<String> metricTypesFromSchema = new ArrayList<>();
    List<String> metricNamesFromConfig = Lists.newArrayList(metricNamesProperty.split(ThirdEyeConstants.FIELD_SEPARATOR));
    for (String metricName : metricNamesFromConfig) {
      metricTypesFromSchema.add(ThirdeyeAvroUtils.getDataTypeForField(metricName, avroSchema));
    }
    String validatedMetricTypesProperty = Joiner.on(ThirdEyeConstants.FIELD_SEPARATOR).join(metricTypesFromSchema);
    if (metricTypesProperty != null) {
      List<String> metricTypesFromConfig = Lists.newArrayList(metricTypesProperty.split(ThirdEyeConstants.FIELD_SEPARATOR));
      if (metricTypesFromConfig.size() == metricTypesFromSchema.size()) {
        for (int i = 0; i < metricNamesFromConfig.size(); i++) {
          String metricName = metricNamesFromConfig.get(i);
          String metricTypeFromConfig = metricTypesFromConfig.get(i);
          String metricTypeFromSchema = metricTypesFromSchema.get(i);
          if (!metricTypeFromConfig.equals(metricTypeFromSchema)) {
            LOGGER.warn("MetricType {} defined in config for metric {}, does not match dataType {} from avro schema",
                metricTypeFromConfig, metricName, metricTypeFromSchema);
          }
        }
        validatedMetricTypesProperty = metricTypesProperty;
      }
    }
    return validatedMetricTypesProperty;
  }

}
