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

import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidatorResult;


/**
 * Schema validator to validate pinot schema and avro schema
 */
public class AvroIngestionSchemaValidator implements IngestionSchemaValidator {
  private org.apache.avro.Schema _avroSchema;
  private Schema _pinotSchema;

  private SchemaValidatorResult _dataTypeMismatch = new SchemaValidatorResult();
  private SchemaValidatorResult _singleValueMultiValueFieldMismatch = new SchemaValidatorResult();
  private SchemaValidatorResult _multiValueStructureMismatch = new SchemaValidatorResult();
  private SchemaValidatorResult _missingPinotColumn = new SchemaValidatorResult();

  public AvroIngestionSchemaValidator() {
  }

  @Override
  public void init(Schema pinotSchema, String inputFilePath) {
    _pinotSchema = pinotSchema;
    _avroSchema = extractAvroSchemaFromFile(inputFilePath);

    validateSchemas();
  }

  @Override
  public String getInputSchemaType() {
    return "AVRO";
  }

  @Override
  public SchemaValidatorResult getDataTypeMismatchResult() {
    return _dataTypeMismatch;
  }

  @Override
  public SchemaValidatorResult getSingleValueMultiValueFieldMismatchResult() {
    return _singleValueMultiValueFieldMismatch;
  }

  @Override
  public SchemaValidatorResult getMultiValueStructureMismatchResult() {
    return _multiValueStructureMismatch;
  }

  @Override
  public SchemaValidatorResult getMissingPinotColumnResult() {
    return _missingPinotColumn;
  }

  private org.apache.avro.Schema extractAvroSchemaFromFile(String inputPath) {
    try {
      DataFileStream<GenericRecord> dataStreamReader = AvroUtils.getAvroReader(new File(inputPath));
      org.apache.avro.Schema avroSchema = dataStreamReader.getSchema();
      dataStreamReader.close();
      return avroSchema;
    } catch (IOException e) {
      throw new RuntimeException("IOException when extracting avro schema from input path: " + inputPath, e);
    }
  }

  private void validateSchemas() {
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }
      String columnName = fieldSpec.getName();
      org.apache.avro.Schema.Field avroColumnField = _avroSchema.getField(columnName);
      if (avroColumnField == null) {
        _missingPinotColumn.addMismatchReason(String
            .format("The Pinot column: (%s: %s) is missing in the %s schema of input data.", columnName,
                fieldSpec.getDataType().name(), getInputSchemaType()));
        continue;
      }

      if (fieldSpec.getDataType() == FieldSpec.DataType.JSON) {
        // No need to carry out further validation if this is a JSON column and we allow for ingesting any AVRO type
        // into JSON column.
        continue;
      }

      String avroColumnName = avroColumnField.schema().getName();
      org.apache.avro.Schema avroColumnSchema = avroColumnField.schema();
      org.apache.avro.Schema.Type avroColumnType = avroColumnSchema.getType();
      if (avroColumnType == org.apache.avro.Schema.Type.UNION) {
        org.apache.avro.Schema nonNullSchema = null;
        for (org.apache.avro.Schema childFieldSchema : avroColumnSchema.getTypes()) {
          if (childFieldSchema.getType() != org.apache.avro.Schema.Type.NULL) {
            if (nonNullSchema == null) {
              nonNullSchema = childFieldSchema;
            } else {
              throw new IllegalStateException("More than one non-null schema in UNION schema");
            }
          }
        }
        if (nonNullSchema != null) {
          avroColumnSchema = nonNullSchema;
          avroColumnType = nonNullSchema.getType();
        }
      }

      if (fieldSpec.isSingleValueField()) {
        // check single-value multi-value mismatch
        if (avroColumnType.ordinal() < org.apache.avro.Schema.Type.STRING.ordinal()) {
          _singleValueMultiValueFieldMismatch.addMismatchReason(String.format(
              "The Pinot column: %s is 'single-value' column but the column: %s from input %s is 'multi-value' column.",
              columnName, avroColumnName, getInputSchemaType()));
        }
        FieldSpec.DataType dataTypeForSVColumn = AvroUtils.extractFieldDataType(avroColumnField);
        // check data type mismatch
        if (fieldSpec.getDataType() != dataTypeForSVColumn) {
          _dataTypeMismatch.addMismatchReason(String
              .format("The Pinot column: (%s: %s) doesn't match with the column (%s: %s) in input %s schema.",
                  columnName, fieldSpec.getDataType().name(), avroColumnName, avroColumnType.name(),
                  getInputSchemaType()));
        }
      } else {
        // check single-value multi-value mismatch
        if (avroColumnType.ordinal() >= org.apache.avro.Schema.Type.STRING.ordinal()) {
          _singleValueMultiValueFieldMismatch.addMismatchReason(String.format(
              "The Pinot column: %s is 'multi-value' column but the column: %s from input %s schema is 'single-value'"
                  + " column.",
              columnName, avroColumnName, getInputSchemaType()));
        }
        // check data type mismatch
        FieldSpec.DataType dataTypeForMVColumn = AvroUtils.extractFieldDataType(avroColumnField);
        if (fieldSpec.getDataType() != dataTypeForMVColumn) {
          _dataTypeMismatch.addMismatchReason(String
              .format("The Pinot column: (%s: %s) doesn't match with the column (%s: %s) in input %s schema.",
                  columnName, fieldSpec.getDataType().name(), avroColumnName, dataTypeForMVColumn.name(),
                  getInputSchemaType()));
        }
        // check multi-value column structure mismatch
        if (avroColumnType != org.apache.avro.Schema.Type.ARRAY) {
          // multi-value column should use array structure for now.
          _multiValueStructureMismatch.addMismatchReason(String.format(
              "The Pinot column: %s is 'multi-value' column but the column: %s from input %s schema is of '%s' type, "
                  + "which should have been of 'array' type.", columnName, avroColumnName, getInputSchemaType(),
              avroColumnType.getName()));
        } else {
          org.apache.avro.Schema.Type elementType = avroColumnSchema.getElementType().getType();
          if (elementType.ordinal() < org.apache.avro.Schema.Type.STRING.ordinal()) {
            // even though the column schema is of array type, the element type of that array could be of complex
            // type like array, map, etc.
            _multiValueStructureMismatch.addMismatchReason(String.format(
                "The Pinot column: %s is 'multi-value' column and it's of 'array' type in input %s schema, but the "
                    + "element type is of '%s' type, which should have been of 'primitive' type.", columnName,
                getInputSchemaType(), avroColumnSchema.getElementType().getType()));
          }
        }
      }
    }
  }
}
