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
package org.apache.pinot.core.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.UuidUtils;


/// Helper methods for Avro conversions used when serializing Pinot [GenericRow]s to Avro — by the file-based
/// `SegmentWriter` implementations and the segment-to-Avro/Parquet converters. (The multi-stage
/// `SegmentProcessorFramework` itself serializes intermediate records with a custom binary `GenericRowFile`
/// format, not Avro.)
public final class SegmentProcessorAvroUtils {

  private SegmentProcessorAvroUtils() {
  }

  /// Convert a GenericRow to an avro GenericRecord
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord) {
    return convertGenericRowToAvroRecord(genericRow, reusableRecord, genericRow.getFieldToValueMap().keySet());
  }

  /// Convert a GenericRow to an avro GenericRecord
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord, Set<String> fields) {
    Schema avroSchema = reusableRecord.getSchema();
    for (String field : fields) {
      Object value = genericRow.getValue(field);
      if (value instanceof Object[]) {
        // Array elements are written as-is. For MV UUID (array<string{logicalType:uuid}>) the elements are the raw
        // 16-byte values; the uuid Conversion registered on the writer's data model (getAvroDataModel) renders each
        // element to its canonical string at write time.
        reusableRecord.put(field, Arrays.asList((Object[]) value));
      } else if (value instanceof byte[]) {
        // A byte[] bound for a plain BYTES field must be wrapped as ByteBuffer (GenericDatumWriter requires it for the
        // bytes type). A byte[] bound for a UUID field (string{logicalType:uuid}) is left raw so the uuid Conversion
        // registered on the writer's data model (getAvroDataModel) renders it to a canonical string at write time.
        Schema.Field avroField = avroSchema.getField(field);
        if (avroField != null && avroField.schema().getType() == Schema.Type.BYTES) {
          reusableRecord.put(field, ByteBuffer.wrap((byte[]) value));
        } else {
          reusableRecord.put(field, value);
        }
      } else {
        reusableRecord.put(field, value);
      }
    }
    return reusableRecord;
  }

  /// Shared Avro data model with [UuidConversion] registered. Populated once at class initialization and never
  /// mutated afterward (effectively immutable), so it is safe to share across writers.
  private static final GenericData AVRO_DATA_MODEL = createAvroDataModel();

  /// Returns the shared Avro data model that a `GenericDatumWriter` (or `AvroParquetWriter`) must be constructed with
  /// to serialize UUID columns produced by [#convertGenericRowToAvroRecord]: it registers [UuidConversion] so the
  /// internal 16-byte UUID form is rendered as the canonical string required by `string{logicalType:uuid}` fields.
  /// The UUID column's field schema must be `string{logicalType:uuid}` — as emitted by
  /// [#convertPinotSchemaToAvroSchema] and `AvroUtils.getAvroSchemaFromPinotSchema` — for the conversion to apply.
  ///
  /// The returned instance is shared and must be treated as read-only: do not call its mutators
  /// (`addLogicalTypeConversion`, `setStringType`, ...), which are not thread-safe against concurrent writer reads.
  public static GenericData getAvroDataModel() {
    return AVRO_DATA_MODEL;
  }

  private static GenericData createAvroDataModel() {
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new UuidConversion());
    return model;
  }

  /// Avro logical-type [Conversion] for the `uuid` logical type, operating directly on Pinot's internal storage form
  /// (a 16-byte big-endian `byte[]`). It renders that value to its canonical RFC-4122 string when writing a
  /// `string{logicalType:uuid}` field and parses it back on read.
  ///
  /// The converted type is `byte[]` rather than [java.util.UUID] so no intermediate object is allocated, and rather
  /// than [java.nio.ByteBuffer] because Avro resolves conversions by exact datum class: a `byte[]` instance always
  /// reports `byte[].class`, so the conversion resolves for both single values and array elements, whereas a
  /// concrete `HeapByteBuffer` would not match a `ByteBuffer`-typed conversion.
  private static final class UuidConversion extends Conversion<byte[]> {
    private static final String UUID_LOGICAL_TYPE_NAME = "uuid";

    @Override
    public Class<byte[]> getConvertedType() {
      return byte[].class;
    }

    @Override
    public String getLogicalTypeName() {
      return UUID_LOGICAL_TYPE_NAME;
    }

    @Override
    public CharSequence toCharSequence(byte[] value, Schema schema, LogicalType type) {
      return UuidUtils.toString(value);
    }

    @Override
    public byte[] fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return UuidUtils.toBytes(value.toString());
    }
  }

  /// Converts a Pinot schema to an Avro schema
  public static Schema convertPinotSchemaToAvroSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    List<FieldSpec> orderedFieldSpecs = pinotSchema.getAllFieldSpecs().stream()
        .sorted(Comparator.comparing(FieldSpec::getName))
        .collect(Collectors.toList());
    for (FieldSpec fieldSpec : orderedFieldSpecs) {
      String name = fieldSpec.getName();
      // Emit UUID columns as Avro string{logicalType:uuid} (matching AvroUtils.getAvroSchemaFromPinotSchema)
      // so the runtime byte[] → canonical-string conversion in convertGenericRowToAvroRecord lines up with
      // the field schema. Without this branch SV UUID would fall through to BYTES (losing UUID semantics) and
      // MV UUID would throw at this point (MV switch below has no BYTES case).
      if (fieldSpec.getDataType() == DataType.UUID) {
        Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        if (fieldSpec.isSingleValueField()) {
          fieldAssembler = fieldAssembler.name(name).type(uuidSchema).noDefault();
        } else {
          fieldAssembler = fieldAssembler.name(name).type().array().items(uuidSchema).noDefault();
        }
        continue;
      }
      DataType storedType = fieldSpec.getDataType().getStoredType();
      if (fieldSpec.isSingleValueField()) {
        switch (storedType) {
          case INT:
            fieldAssembler = fieldAssembler.name(name).type().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(name).type().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(name).type().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(name).type().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(name).type().stringType().noDefault();
            break;
          case BYTES:
            fieldAssembler = fieldAssembler.name(name).type().bytesType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      } else {
        switch (storedType) {
          case INT:
            fieldAssembler = fieldAssembler.name(name).type().array().items().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(name).type().array().items().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(name).type().array().items().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(name).type().array().items().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(name).type().array().items().stringType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      }
    }
    return fieldAssembler.endRecord();
  }
}
