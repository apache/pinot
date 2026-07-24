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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
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

  /// Convert a GenericRow to an avro GenericRecord.
  ///
  /// Values arrive in Pinot's internal (stored) representation and are coordinated with the Avro field type produced
  /// by `AvroSchemaUtil.toAvroSchema`: whatever a registered logical-type [Conversion] can handle is left untouched
  /// for the writer, and only the two cases Avro cannot resolve on its own are fixed up here (see
  /// [#convertValue(Schema, Object)]).
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord, Set<String> fields) {
    Schema avroSchema = reusableRecord.getSchema();
    for (String field : fields) {
      Object value = genericRow.getValue(field);
      Schema.Field avroField = avroSchema.getField(field);
      if (avroField == null) {
        // Let Avro raise its own "Not a valid schema field" error for a column missing from the Avro schema.
        reusableRecord.put(field, value);
      } else {
        reusableRecord.put(avroField.pos(), convertValue(avroField.schema(), value));
      }
    }
    return reusableRecord;
  }

  /// Adapts a Pinot value to the representation the Avro writer expects for the given field schema, recursing into
  /// array elements for multi-value columns.
  @Nullable
  private static Object convertValue(Schema fieldSchema, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Object[]) {
      Object[] values = (Object[]) value;
      Schema elementSchema =
          fieldSchema.getType() == Schema.Type.ARRAY ? fieldSchema.getElementType() : fieldSchema;
      // Only BOOLEAN (stored int -> Boolean) and BYTES (byte[] -> ByteBuffer) element schemas can require a
      // per-element transform. Every other MV element type is written as-is — INT/LONG/FLOAT/DOUBLE/STRING directly,
      // and UUID (string element, raw byte[] rendered by the registered Conversion) — so hand the writer a zero-copy
      // view over the existing array; allocating and copying a fresh list per row would be pure overhead on the
      // segment-write hot path. (BIG_DECIMAL has a BYTES element schema and so takes the copy path below, but its
      // BigDecimal values still pass through convertSingleValue unchanged for the registered Conversion.)
      Schema.Type elementType = elementSchema.getType();
      if (elementType != Schema.Type.BOOLEAN && elementType != Schema.Type.BYTES) {
        return Arrays.asList(values);
      }
      List<Object> converted = new ArrayList<>(values.length);
      for (Object singleValue : values) {
        converted.add(convertSingleValue(elementSchema, singleValue));
      }
      return converted;
    }
    return convertSingleValue(fieldSchema, value);
  }

  /// Adapts a single (non-array) Pinot value to what `GenericDatumWriter` expects for `valueSchema`.
  ///
  /// Only two cases need fixing up; everything else is written as-is, either because the Pinot representation already
  /// *is* the Avro representation (`Integer` for `int`, `Long` for `long{timestamp-millis}`, `String` for `string`)
  /// or because a [Conversion] registered on [#getAvroDataModel] handles it (`byte[]` for `string{uuid}`,
  /// [java.math.BigDecimal] for `bytes{big-decimal}`).
  @Nullable
  private static Object convertSingleValue(Schema valueSchema, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    switch (valueSchema.getType()) {
      case BOOLEAN:
        // BOOLEAN is the one Pinot logical type with no Avro logical type to carry a Conversion, so its stored int
        // 0/1 has to be coerced here. Values that are already Boolean (e.g. from a source that never went through
        // Pinot's stored form) pass through.
        return value instanceof Boolean ? value : ((Number) value).intValue() != 0;
      case BYTES:
        // A byte[] bound for a plain BYTES field must be wrapped as ByteBuffer (GenericDatumWriter requires it for
        // the bytes type). A BigDecimal bound for bytes{big-decimal} is left alone for the registered Conversion.
        return value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) : value;
      default:
        return value;
    }
  }

  /// Shared Avro data model with the logical-type conversions registered. Populated once at class initialization and
  /// never mutated afterward (effectively immutable), so it is safe to share across writers.
  private static final GenericData AVRO_DATA_MODEL = createAvroDataModel();

  /// Returns the shared Avro data model that a `GenericDatumWriter` (or `AvroParquetWriter`) must be constructed with
  /// to serialize the logical-type columns produced by [#convertGenericRowToAvroRecord]:
  /// - [UuidConversion] renders Pinot's internal 16-byte UUID form as the canonical string required by
  ///   `string{logicalType:uuid}` fields.
  /// - Avro's `BigDecimalConversion` encodes a [java.math.BigDecimal] — unscaled value plus its own scale — into the
  ///   `bytes{logicalType:big-decimal}` fields Pinot emits for BIG_DECIMAL columns.
  ///
  /// The column's field schema must be the one emitted by [#convertPinotSchemaToAvroSchema] /
  /// `AvroUtils.getAvroSchemaFromPinotSchema` for the conversions to apply. BOOLEAN and TIMESTAMP need no entry
  /// here: Avro has no `boolean` logical type (the coercion happens in [#convertGenericRowToAvroRecord]), and a
  /// `Long` already *is* the base representation of `long{logicalType:timestamp-millis}`.
  ///
  /// The returned instance is shared and must be treated as read-only: do not call its mutators
  /// (`addLogicalTypeConversion`, `setStringType`, ...), which are not thread-safe against concurrent writer reads.
  public static GenericData getAvroDataModel() {
    return AVRO_DATA_MODEL;
  }

  private static GenericData createAvroDataModel() {
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new UuidConversion());
    model.addLogicalTypeConversion(new Conversions.BigDecimalConversion());
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

  /// Converts a Pinot schema to an Avro schema, with the fields ordered by column name.
  ///
  /// Field types are derived from the **original (logical)** Pinot data type, so BOOLEAN becomes Avro `boolean`,
  /// TIMESTAMP a `timestamp-millis` long, BIG_DECIMAL a `big-decimal` bytes and UUID a `uuid` string, instead of all
  /// four collapsing to their physical storage type. This must stay identical to
  /// `AvroSchemaUtil.toAvroSchema(FieldSpec)` in `pinot-avro-base` — the two live in different modules (neither can
  /// depend on the other) but feed the same writers, so `SegmentProcessorAvroUtilsTest` pins them together. See that
  /// method for the full mapping table and the value representation each Avro type expects.
  public static Schema convertPinotSchemaToAvroSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler = SchemaBuilder.record("record").fields();
    List<FieldSpec> orderedFieldSpecs = pinotSchema.getAllFieldSpecs().stream()
        .sorted(Comparator.comparing(FieldSpec::getName))
        .collect(Collectors.toList());
    for (FieldSpec fieldSpec : orderedFieldSpecs) {
      fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type(toAvroSchema(fieldSpec)).noDefault();
    }
    return fieldAssembler.endRecord();
  }

  /// Returns the Avro schema for a whole Pinot column: the single-value type from [#toAvroSchema(DataType)], or an
  /// array of it for a multi-value column.
  private static Schema toAvroSchema(FieldSpec fieldSpec) {
    Schema valueSchema = toAvroSchema(fieldSpec.getDataType());
    return fieldSpec.isSingleValueField() ? valueSchema : Schema.createArray(valueSchema);
  }

  private static Schema toAvroSchema(DataType dataType) {
    switch (dataType) {
      case INT:
        return Schema.create(Schema.Type.INT);
      case LONG:
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case TIMESTAMP:
        return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      case BIG_DECIMAL:
        return LogicalTypes.bigDecimal().addToSchema(Schema.create(Schema.Type.BYTES));
      case STRING:
      case JSON:
        return Schema.create(Schema.Type.STRING);
      case BYTES:
        return Schema.create(Schema.Type.BYTES);
      case UUID:
        return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }
}
