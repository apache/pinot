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
package org.apache.pinot.integration.tests.udf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


/// Like SegmentProcessorAvroUtils, but:
/// - Assumes generic rows will use normal java types instead of stored types (ie booleans are not stored as ints)
/// - Uses normal avro types instead of stored types (ie booleans are written as booleans, big decimals as bytes,
/// etc.)
/// - Internally manages the Avro objects
/// - Defines a sink like external interface,
public class AvroSink implements AutoCloseable {

  private static final EnumMap<FieldSpec.DataType, Schema> NOT_NULL_SCALAR_MAP;
  private static final EnumMap<FieldSpec.DataType, Schema> NULL_SCALAR_MAP;
  private static final EnumMap<FieldSpec.DataType, Schema> NOT_NULL_MULTI_VALUE_MAP;
  private static final EnumMap<FieldSpec.DataType, Schema> NULL_MULTI_VALUE_MAP;

  static {
    NOT_NULL_SCALAR_MAP = new EnumMap<>(FieldSpec.DataType.class);
    NULL_SCALAR_MAP = new EnumMap<>(FieldSpec.DataType.class);
    NOT_NULL_MULTI_VALUE_MAP = new EnumMap<>(FieldSpec.DataType.class);
    NULL_MULTI_VALUE_MAP = new EnumMap<>(FieldSpec.DataType.class);

    Schema nullSchema = Schema.create(Schema.Type.NULL);
    for (FieldSpec.DataType value : FieldSpec.DataType.values()) {
      switch (value) {
        case INT:
          addType(value, Schema.create(Schema.Type.INT), nullSchema);
          break;
        case LONG:
          addType(value, Schema.create(Schema.Type.LONG), nullSchema);
          break;
        case FLOAT:
          addType(value, Schema.create(Schema.Type.FLOAT), nullSchema);
          break;
        case DOUBLE:
          addType(value, Schema.create(Schema.Type.DOUBLE), nullSchema);
          break;
        case STRING:
        case JSON:
          addType(value, Schema.create(Schema.Type.STRING), nullSchema);
          break;
        case BIG_DECIMAL:
          Schema bigDecimal = LogicalTypes.bigDecimal().addToSchema(SchemaBuilder.builder().bytesType());
          addType(value, bigDecimal, nullSchema);
          break;
        case BYTES:
          addType(value, Schema.create(Schema.Type.BYTES), nullSchema);
          break;
        case BOOLEAN:
          addType(value, Schema.create(Schema.Type.BOOLEAN), nullSchema);
          break;
        case TIMESTAMP:
          Schema timestampMillis = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
          addType(value, timestampMillis, nullSchema);
          break;
        case MAP:
        case LIST:
        case STRUCT:
        case UNKNOWN:
          // Types we know we don't support in AVRO
          break;
        default:
          throw new RuntimeException("Unsupported data type: " + value);
      }
    }
  }

  private final Schema _avroSchema;
  private final DataFileWriter<GenericRecord> _dataFileWriter;

  public AvroSink(org.apache.pinot.spi.data.Schema schema, File tempFile)
      throws IOException {
    _avroSchema = convertPinotSchemaToAvroSchema(schema);

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(_avroSchema, AvroUtils.getGenericData());
    _dataFileWriter = new DataFileWriter<>(datumWriter);
    _dataFileWriter.create(_avroSchema, tempFile);
  }

  public void consume(GenericRow row)
      throws IOException {
    GenericData.Record record = new GenericData.Record(_avroSchema);
    convertGenericRowToAvroRecord(row, record);
    _dataFileWriter.append(record);
  }

  @Override
  public void close()
      throws IOException {
    _dataFileWriter.close();
  }

  private static void addType(
      FieldSpec.DataType dataType, Schema scalarSchema, Schema nullSchema) {
    NOT_NULL_SCALAR_MAP.put(dataType, scalarSchema);
    Schema nullableSchema = Schema.createUnion(scalarSchema, nullSchema);
    NULL_SCALAR_MAP.put(dataType, nullableSchema);
    Schema multiValueSchema = Schema.createArray(nullableSchema);
    NOT_NULL_MULTI_VALUE_MAP.put(dataType, multiValueSchema);
    NULL_MULTI_VALUE_MAP.put(dataType, Schema.createUnion(multiValueSchema, nullSchema));
  }

  private static void convertGenericRowToAvroRecord(GenericRow input,
      GenericData.Record output) {
    for (String field : input.getFieldToValueMap().keySet()) {
      Object value = input.getValue(field);
      if (value instanceof Object[]) {
        output.put(field, Arrays.asList((Object[]) value));
      } else {
        if (value instanceof byte[]) {
          value = ByteBuffer.wrap((byte[]) value);
        }
        output.put(field, value);
      }
    }
  }

  /**
   * Converts a Pinot schema to an Avro schema
   */
  private static Schema convertPinotSchemaToAvroSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    List<FieldSpec> orderedFieldSpecs = pinotSchema.getAllFieldSpecs().stream()
        .sorted(Comparator.comparing(FieldSpec::getName))
        .collect(Collectors.toList());
    for (FieldSpec fieldSpec : orderedFieldSpecs) {
      String name = fieldSpec.getName();
      FieldSpec.DataType dataType = fieldSpec.getDataType();

      Schema fieldType;
      if (fieldSpec.isSingleValueField()) {
        if (fieldSpec.isNullable()) {
          fieldType = NULL_SCALAR_MAP.get(dataType);
        } else {
          fieldType = NOT_NULL_SCALAR_MAP.get(dataType);
        }
      } else {
        if (fieldSpec.isNullable()) {
          fieldType = NULL_MULTI_VALUE_MAP.get(dataType);
        } else {
          fieldType = NOT_NULL_MULTI_VALUE_MAP.get(dataType);
        }
      }
      if (fieldType == null) {
        throw new RuntimeException("Unsupported data type: " + dataType);
      }

      fieldAssembler.name(name)
          .type(fieldType)
          .noDefault();
    }
    return fieldAssembler.endRecord();
  }
}
