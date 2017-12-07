/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.util;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;


public class AvroUtils {
  private AvroUtils() {
  }

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
    try (DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile)) {
      org.apache.avro.Schema avroSchema = dataStreamReader.getSchema();
      return getPinotSchemaFromAvroSchema(avroSchema, getDefaultFieldTypes(avroSchema), TimeUnit.DAYS);
    }
  }

  /**
   * This is just a refactor of the original code that had the hard-coded logic for deducing
   * if a column is dimension/metric/time. This is used only for testing purposes.
   *
   * @param avroSchema The input avro schema for which to deduce the dimension/metric/time columns.
   * @return Hash map containing column names as keys and field type (dim/metric/time) as value.
   */
  private static Map<String, FieldSpec.FieldType> getDefaultFieldTypes(org.apache.avro.Schema avroSchema) {
    Map<String, FieldSpec.FieldType> fieldTypes = new HashMap<>();

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
      Map<String, FieldSpec.FieldType> fieldTypes, TimeUnit timeUnit) {
    Schema pinotSchema = new Schema();

    for (Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      FieldSpec.DataType dataType = extractFieldDataType(field);
      boolean isSingleValueField = isSingleValueField(field);

      FieldSpec.FieldType fieldType = fieldTypes.get(fieldName);
      switch (fieldType) {
        case DIMENSION:
          pinotSchema.addField(new DimensionFieldSpec(fieldName, dataType, isSingleValueField));
          break;
        case METRIC:
          Preconditions.checkState(isSingleValueField, "Unsupported multi-value for metric field.");
          pinotSchema.addField(new MetricFieldSpec(fieldName, dataType));
          break;
        case TIME:
          Preconditions.checkState(isSingleValueField, "Unsupported multi-value for time field.");
          pinotSchema.addField(new TimeFieldSpec(field.name(), dataType, timeUnit));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType + " for field: " + fieldName);
      }
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
  public static Schema getPinotSchemaFromAvroSchemaFile(String avroSchemaFileName,
      Map<String, FieldSpec.FieldType> fieldTypes, TimeUnit timeUnit) throws IOException {
    File avroSchemaFile = new File(avroSchemaFileName);
    if (!avroSchemaFile.exists()) {
      throw new FileNotFoundException();
    }

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaFile);
    return getPinotSchemaFromAvroSchema(avroSchema, fieldTypes, timeUnit);
  }

  /**
   * Get the Avro file reader for the given file.
   */
  public static DataFileStream<GenericRecord> getAvroReader(File avroFile) throws IOException {
    if (avroFile.getName().endsWith(".gz")) {
      return new DataFileStream<>(new GZIPInputStream(new FileInputStream(avroFile)),
          new GenericDatumReader<GenericRecord>());
    } else {
      return new DataFileStream<>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    }
  }

  /**
   * Return whether the Avro field is a single-value field.
   */
  public static boolean isSingleValueField(Field field) {
    try {
      org.apache.avro.Schema fieldSchema = extractSupportedSchema(field.schema());
      return fieldSchema.getType() != org.apache.avro.Schema.Type.ARRAY;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while extracting non-null schema from field: " + field.name(), e);
    }
  }

  /**
   * Extract the data type stored in Pinot for the given Avro field.
   */
  public static FieldSpec.DataType extractFieldDataType(Field field) {
    try {
      org.apache.avro.Schema fieldSchema = extractSupportedSchema(field.schema());
      org.apache.avro.Schema.Type fieldType = fieldSchema.getType();
      if (fieldType == org.apache.avro.Schema.Type.ARRAY) {
        return FieldSpec.DataType.valueOf(extractSupportedSchema(fieldSchema.getElementType()).getType());
      } else {
        return FieldSpec.DataType.valueOf(fieldType);
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while extracting data type from field: " + field.name(), e);
    }
  }

  /**
   * Helper method to extract the supported Avro schema from the given Avro field schema.
   * <p>Currently we support INT/LONG/FLOAT/DOUBLE/BOOLEAN/STRING/ENUM
   */
  private static org.apache.avro.Schema extractSupportedSchema(org.apache.avro.Schema fieldSchema) {
    org.apache.avro.Schema.Type fieldType = fieldSchema.getType();
    if (fieldType == org.apache.avro.Schema.Type.UNION) {
      org.apache.avro.Schema nonNullSchema = null;
      for (org.apache.avro.Schema childFieldSchema : fieldSchema.getTypes()) {
        if (childFieldSchema.getType() != org.apache.avro.Schema.Type.NULL) {
          if (nonNullSchema == null) {
            nonNullSchema = childFieldSchema;
          } else {
            throw new IllegalStateException("More than one non-null schema in UNION schema");
          }
        }
      }
      if (nonNullSchema != null) {
        return extractSupportedSchema(nonNullSchema);
      } else {
        throw new IllegalStateException("Cannot find non-null schema in UNION schema");
      }
    } else if (fieldType == org.apache.avro.Schema.Type.RECORD) {
      List<Field> recordFields = fieldSchema.getFields();
      Preconditions.checkState(recordFields.size() == 1, "Not one field in the RECORD schema");
      return extractSupportedSchema(recordFields.get(0).schema());
    } else {
      return fieldSchema;
    }
  }

  /**
   * Fill the data in a {@link GenericRecord} to a {@link GenericRow}.
   */
  public static void fillGenericRow(GenericRecord from, GenericRow to, Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      Object avroValue = from.get(fieldName);
      if (fieldSpec.isSingleValueField()) {
        to.putField(fieldName, transformAvroValueToObject(avroValue, fieldSpec));
      } else {
        to.putField(fieldName, transformAvroArrayToObjectArray((GenericData.Array) avroValue, fieldSpec));
      }
    }
  }

  /**
   * Transform a single-value Avro value into an object in Pinot format.
   */
  public static Object transformAvroValueToObject(Object avroValue, FieldSpec fieldSpec) {
    if (avroValue == null) {
      return fieldSpec.getDefaultNullValue();
    }
    if (avroValue instanceof GenericData.Record) {
      return transformAvroValueToObject(((GenericData.Record) avroValue).get(0), fieldSpec);
    }
    if (fieldSpec.getDataType() == FieldSpec.DataType.STRING) {
      return avroValue.toString();
    }
    return avroValue;
  }

  /**
   * Transform an Avro array into an object array in Pinot format.
   */
  public static Object[] transformAvroArrayToObjectArray(GenericData.Array avroArray, FieldSpec fieldSpec) {
    if (avroArray == null || avroArray.size() == 0) {
      return new Object[]{fieldSpec.getDefaultNullValue()};
    }
    int numValues = avroArray.size();
    Object[] objects = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      objects[i] = transformAvroValueToObject(avroArray.get(i), fieldSpec);
    }
    return objects;
  }
}
