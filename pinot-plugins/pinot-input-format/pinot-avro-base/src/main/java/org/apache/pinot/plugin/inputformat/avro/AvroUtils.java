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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils for handling Avro records
 */
public class AvroUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtils.class);

  private AvroUtils() {
  }

  /**
   * Given an Avro schema, map from column to field type and time unit, return the equivalent Pinot schema.
   *
   * @param avroSchema Avro schema
   * @param fieldTypeMap Map from column to field type
   * @param timeUnit Time unit
   * @return Pinot schema
   */
  public static Schema getPinotSchemaFromAvroSchema(org.apache.avro.Schema avroSchema,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit) {
    Schema pinotSchema = new Schema();

    for (Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      FieldSpec.DataType dataType = extractFieldDataType(field);
      boolean isSingleValueField = isSingleValueField(field);
      if (fieldTypeMap == null) {
        pinotSchema.addField(new DimensionFieldSpec(fieldName, dataType, isSingleValueField));
      } else {
        FieldSpec.FieldType fieldType = fieldTypeMap.get(fieldName);
        Preconditions.checkNotNull(fieldType, "Field type not specified for field: %s", fieldName);
        switch (fieldType) {
          case DIMENSION:
            pinotSchema.addField(new DimensionFieldSpec(fieldName, dataType, isSingleValueField));
            break;
          case METRIC:
            Preconditions.checkState(isSingleValueField, "Metric field: %s cannot be multi-valued", fieldName);
            pinotSchema.addField(new MetricFieldSpec(fieldName, dataType));
            break;
          case TIME:
            Preconditions.checkState(isSingleValueField, "Time field: %s cannot be multi-valued", fieldName);
            Preconditions.checkNotNull(timeUnit, "Time unit cannot be null");
            pinotSchema.addField(new TimeFieldSpec(new TimeGranularitySpec(dataType, timeUnit, field.name())));
            break;
          case DATE_TIME:
            Preconditions.checkState(isSingleValueField, "Time field: %s cannot be multi-valued", fieldName);
            Preconditions.checkNotNull(timeUnit, "Time unit cannot be null");
            pinotSchema.addField(new DateTimeFieldSpec(field.name(), dataType,
                new DateTimeFormatSpec(1, timeUnit.toString(), DateTimeFieldSpec.TimeFormat.EPOCH.toString())
                    .getFormat(), new DateTimeGranularitySpec(1, timeUnit).getGranularity()));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported field type: " + fieldType + " for field: " + fieldName);
        }
      }
    }

    return pinotSchema;
  }

  /**
   * Given an Avro data file, map from column to field type and time unit, return the equivalent Pinot schema.
   *
   * @param avroDataFile Avro data file
   * @param fieldTypeMap Map from column to field type
   * @param timeUnit Time unit
   * @return Pinot schema
   */
  public static Schema getPinotSchemaFromAvroDataFile(File avroDataFile,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit)
      throws IOException {
    try (DataFileStream<GenericRecord> reader = getAvroReader(avroDataFile)) {
      org.apache.avro.Schema avroSchema = reader.getSchema();
      return getPinotSchemaFromAvroSchema(avroSchema, fieldTypeMap, timeUnit);
    }
  }

  /**
   * Given an Avro data file, count all columns as dimension and return the equivalent Pinot schema.
   * <p>Should be used for testing purpose only.
   *
   * @param avroDataFile Avro data file
   * @return Pinot schema
   */
  public static Schema getPinotSchemaFromAvroDataFile(File avroDataFile)
      throws IOException {
    return getPinotSchemaFromAvroDataFile(avroDataFile, null, null);
  }

  /**
   * Given an Avro schema file, map from column to field type and time unit, return the equivalent Pinot schema.
   *
   * @param avroSchemaFile Avro schema file
   * @param fieldTypeMap Map from column to field type
   * @param timeUnit Time unit
   * @return Pinot schema
   */
  public static Schema getPinotSchemaFromAvroSchemaFile(File avroSchemaFile,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit)
      throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaFile);
    return getPinotSchemaFromAvroSchema(avroSchema, fieldTypeMap, timeUnit);
  }

  /**
   * Helper method to build Avro schema from Pinot schema.
   *
   * @param pinotSchema Pinot schema.
   * @return Avro schema.
   */
  public static org.apache.avro.Schema getAvroSchemaFromPinotSchema(Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      if (fieldSpec.isSingleValueField()) {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().stringType().noDefault();
            break;
          case BYTES:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().bytesType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      } else {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().stringType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      }
    }

    return fieldAssembler.endRecord();
  }

  /**
   * Get the Avro file reader for the given file.
   */
  public static DataFileStream<GenericRecord> getAvroReader(File avroFile)
      throws IOException {
    if (avroFile.getName().endsWith(".gz")) {
      return new DataFileStream<>(new GZIPInputStream(new FileInputStream(avroFile)), new GenericDatumReader<>());
    } else {
      return new DataFileStream<>(new FileInputStream(avroFile), new GenericDatumReader<>());
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
        return AvroSchemaUtil.valueOf(extractSupportedSchema(fieldSchema.getElementType()).getType());
      } else {
        return AvroSchemaUtil.valueOf(fieldType);
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
}
