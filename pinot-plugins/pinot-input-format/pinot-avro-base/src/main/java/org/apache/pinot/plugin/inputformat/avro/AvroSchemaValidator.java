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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidator;

/**
 * Schema validator to validate pinot schema and avro schema
 */
public class AvroSchemaValidator implements SchemaValidator {
  private org.apache.avro.Schema _avroSchema;
  private Schema _pinotSchema;

  private int _dataTypeMismatch = 0;
  private int _singleValueMultiValueFieldMismatch = 0;
  private int _multiValueStructureMismatch = 0;
  private int _missingPinotColumn = 0;

  public AvroSchemaValidator() {
  }

  @Override
  public void init(Schema pinotSchema, String inputFilePath) {
    _pinotSchema = pinotSchema;
    _avroSchema = extractAvroSchemaFromFile(inputFilePath);

    validateSchemas();
  }

  @Override
  public boolean isDataTypeMismatch() {
    return _dataTypeMismatch != 0;
  }

  @Override
  public boolean isSingleValueMultiValueFieldMismatch() {
    return _singleValueMultiValueFieldMismatch != 0;
  }

  @Override
  public boolean isMultiValueStructureMismatch() {
    return _multiValueStructureMismatch != 0;
  }

  @Override
  public boolean isMissingPinotColumn() {
    return _missingPinotColumn != 0;
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
    for (String columnName : _pinotSchema.getPhysicalColumnNames()) {
      FieldSpec fieldSpec = _pinotSchema.getFieldSpecFor(columnName);
      org.apache.avro.Schema.Field avroColumnField = _avroSchema.getField(columnName);
      if (avroColumnField == null) {
        _missingPinotColumn++;
        continue;
      }
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
          avroColumnType = nonNullSchema.getType();
        }
      }

      if (!fieldSpec.getDataType().name().equalsIgnoreCase(avroColumnType.toString())) {
        _dataTypeMismatch++;
      }

      if (fieldSpec.isSingleValueField()) {
        if (avroColumnType.ordinal() < org.apache.avro.Schema.Type.STRING.ordinal()) {
          // the column is a complex structure
          _singleValueMultiValueFieldMismatch++;
        }
      } else {
        if (avroColumnType.ordinal() >= org.apache.avro.Schema.Type.STRING.ordinal()) {
          // the column is a complex structure
          _singleValueMultiValueFieldMismatch++;
        }
        if (avroColumnType != org.apache.avro.Schema.Type.ARRAY) {
          // multi-value column should use array structure for now.
          _multiValueStructureMismatch++;
        }
      }
    }
  }
}
