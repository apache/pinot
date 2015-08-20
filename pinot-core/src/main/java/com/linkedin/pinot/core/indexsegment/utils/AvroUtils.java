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
package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Aug 19, 2014
 */
public class AvroUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtils.class);

  private static final String COUNT = "count";
  private static final String METRIC = "met";
  private static final String DAY = "days";
  private static final String DAYS_SINCE_EPOCH = "daysSinceEpoch";

  /**
   * gives back a basic pinot schema object with field type as unknown and not aware of whether SV or MV
   * this is just a util method for testing
   * @param avroFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static Schema extractSchemaFromAvro(File avroFile) throws IOException {

    final Schema schema = new Schema();
    final DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile);
    final org.apache.avro.Schema avroSchema = dataStreamReader.getSchema();
    dataStreamReader.close();

    return getPinotSchemaFromAvroSchema(avroSchema, getDefaultFieldTypes(avroSchema), TimeUnit.DAYS);
  }


  /**
   * This is just a refactor of the original code that had the hard-coded logic for deducing
   * if a column is dimension/metric/time. This is used only for testing purposes.
   *
   * @param avroSchema The input avro schema for which to deduce the dimension/metric/time columns.
   * @return Hash map containing column names as keys and field type (dim/metric/time) as value.
   */
  private static Map<String, FieldSpec.FieldType> getDefaultFieldTypes(org.apache.avro.Schema avroSchema) {
    Map<String, FieldSpec.FieldType> fieldTypes = new HashMap<String, FieldSpec.FieldType>();

    for (final Field field : avroSchema.getFields()) {
      FieldSpec.FieldType fieldType;

      if (field.name().contains(COUNT) || field.name().contains(METRIC)) {
        fieldType = FieldSpec.FieldType.METRIC;
      } else if (field.name().contains(DAY) || field.name().equalsIgnoreCase(DAYS_SINCE_EPOCH)) {
        fieldType = FieldSpec.FieldType.TIME;
      } else {
        fieldType = FieldSpec.FieldType.DIMENSION;
      }

      fieldTypes.put(field.name(), fieldType);
    }

    return fieldTypes;
  }

  /**
   * Given an avro schema object along with column field types and time unit, return the equivalent
   * pinot schema object.
   *
   * @param avroSchema Avro schema for which to get the Pinot schema.
   * @param fieldTypes Map containing fieldTypes for each column.
   * @param timeUnit Time unit to be used for the time column.
   * @return Return the equivalent pinot schema for the given avro schema.
   */
  private static Schema getPinotSchemaFromAvroSchema(org.apache.avro.Schema avroSchema,
                                                     Map<String, FieldSpec.FieldType> fieldTypes,
                                                     TimeUnit timeUnit) {
    Schema pinotSchema = new Schema();

    for (final Field field : avroSchema.getFields()) {
      FieldSpec spec;
      FieldSpec.DataType columnType;

      try {
        columnType = AvroRecordReader.getColumnType(field);
      } catch (UnsupportedOperationException e) {
        LOGGER.warn("Unsupported field type for field {}, using String instead.", field.name());
        columnType = FieldSpec.DataType.STRING;
      }

      FieldSpec.FieldType fieldType = fieldTypes.get(field.name());
      if (fieldType == FieldSpec.FieldType.METRIC) {
        spec = new MetricFieldSpec();
        spec.setDataType(columnType);
      } else if (fieldType == FieldSpec.FieldType.TIME) {
        spec = new TimeFieldSpec(field.name(), columnType, timeUnit);
      } else {
        spec = new DimensionFieldSpec();
        spec.setDataType(columnType);
      }

      spec.setName(field.name());
      spec.setSingleValueField(AvroRecordReader.isSingleValueField(field));

      pinotSchema.addSchema(spec.getName(), spec);
    }
    return pinotSchema;
  }

  /**
   * Given a avro schema file name, field types for columns and type unit, return the equivalent
   * pinot schema object.
   *
   * @param avroSchemaFileName Name of the text file containing avro schema.
   * @return PinotSchema equivalent of avro schema.
   */
  public static Schema getPinotSchemaFromAvroSchemaFile(String avroSchemaFileName, Map<String,
      FieldSpec.FieldType> fieldTypes, TimeUnit timeUnit) throws IOException {
    File avroSchemaFile = new File(avroSchemaFileName);
    if (!avroSchemaFile.exists()) {
      throw new FileNotFoundException();
    }

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaFile);
    return getPinotSchemaFromAvroSchema(avroSchema, fieldTypes, timeUnit);
  }

  public static List<String> getAllColumnsInAvroFile(File avroFile) throws IOException {
    final List<String> ret = new ArrayList<String>();
    final DataFileStream<GenericRecord> reader = getAvroReader(avroFile);
    for (final Field f : reader.getSchema().getFields()) {
      ret.add(f.name());
    }
    reader.close();
    return ret;
  }

  public static DataFileStream<GenericRecord> getAvroReader(File avroFile) throws IOException {
    if(avroFile.getName().endsWith("gz"))
      return new DataFileStream<GenericRecord>(new GZIPInputStream(new FileInputStream(avroFile)), new GenericDatumReader<GenericRecord>());
    else
      return new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
  }

}
