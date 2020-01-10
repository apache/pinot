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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParquetUtils {
  public static final String DEFAULT_FS = "file:///";
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetUtils.class);

  /**
   * Returns a ParquetReader with the given path.
   */
  public static ParquetReader<GenericRecord> getParquetReader(Path path)
      throws IOException {
    //noinspection unchecked
    return AvroParquetReader.<GenericRecord>builder(path).disableCompatibility().withDataModel(GenericData.get())
        .withConf(getConfiguration()).build();
  }

  /**
   * Returns a ParquetWriter with the given path and schema.
   */
  public static ParquetWriter<GenericRecord> getParquetWriter(Path path, Schema schema)
      throws IOException {
    return AvroParquetWriter.<GenericRecord>builder(path).withSchema(schema).withConf(getConfiguration()).build();
  }

  /**
   * Returns the schema for the given Parquet file path.
   */
  public static Schema getParquetSchema(Path path)
      throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(getConfiguration(), path, ParquetMetadataConverter.NO_FILTER);
    Map<String, String> metaData = footer.getFileMetaData().getKeyValueMetaData();
    String schemaString = metaData.get("parquet.avro.schema");
    if (schemaString == null) {
      // Try the older property
      schemaString = metaData.get("avro.schema");
    }
    if (schemaString != null) {
      return new Schema.Parser().parse(schemaString);
    } else {
      return new AvroSchemaConverter().convert(footer.getFileMetaData().getSchema());
    }
  }

  private static Configuration getConfiguration() {
    // The file path used in ParquetRecordReader is a local file path without prefix 'file:///',
    // so we have to make sure that the configuration item 'fs.defaultFS' is set to 'file:///'
    // in case that user's hadoop conf overwrite this item
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_FS);
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return conf;
  }

  /**
   * Helper method to build Avro schema from Pinot schema.
   *
   * @param pinotSchema Pinot schema.
   * @return Avro schema.
   */
  public static org.apache.avro.Schema getAvroSchemaFromPinotSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
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
   * Validates Pinot schema against the given Avro schema.
   */
  public static void validateSchema(org.apache.pinot.spi.data.Schema schema, org.apache.avro.Schema avroSchema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      Schema.Field avroField = avroSchema.getField(fieldName);
      if (avroField == null) {
        LOGGER.warn("Pinot field: {} does not exist in Avro Schema", fieldName);
      } else {
        boolean isPinotFieldSingleValue = fieldSpec.isSingleValueField();
        boolean isAvroFieldSingleValue = isSingleValueField(avroField);
        if (isPinotFieldSingleValue != isAvroFieldSingleValue) {
          String errorMessage = "Pinot field: " + fieldName + " is " + (isPinotFieldSingleValue ? "Single" : "Multi")
              + "-valued in Pinot schema but not in Avro schema";
          LOGGER.error(errorMessage);
          throw new IllegalStateException(errorMessage);
        }

        FieldSpec.DataType pinotFieldDataType = fieldSpec.getDataType();
        FieldSpec.DataType avroFieldDataType = extractFieldDataType(avroField);
        if (pinotFieldDataType != avroFieldDataType) {
          LOGGER.warn("Pinot field: {} of type: {} mismatches with corresponding field in Avro Schema of type: {}",
              fieldName, pinotFieldDataType, avroFieldDataType);
        }
      }
    }
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
  public static boolean isSingleValueField(Schema.Field field) {
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
  public static FieldSpec.DataType extractFieldDataType(Schema.Field field) {
    try {
      org.apache.avro.Schema fieldSchema = extractSupportedSchema(field.schema());
      org.apache.avro.Schema.Type fieldType = fieldSchema.getType();
      if (fieldType == org.apache.avro.Schema.Type.ARRAY) {
        return valueOf(extractSupportedSchema(fieldSchema.getElementType()).getType());
      } else {
        return valueOf(fieldType);
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
      List<Schema.Field> recordFields = fieldSchema.getFields();
      Preconditions.checkState(recordFields.size() == 1, "Not one field in the RECORD schema");
      return extractSupportedSchema(recordFields.get(0).schema());
    } else {
      return fieldSchema;
    }
  }

  /**
   * Returns the data type stored in Pinot that is associated with the given Avro type.
   */
  public static FieldSpec.DataType valueOf(org.apache.avro.Schema.Type avroType) {
    switch (avroType) {
      case INT:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BOOLEAN:
      case STRING:
      case ENUM:
        return FieldSpec.DataType.STRING;
      case BYTES:
        return FieldSpec.DataType.BYTES;
      default:
        throw new UnsupportedOperationException("Unsupported Avro type: " + avroType);
    }
  }
}
