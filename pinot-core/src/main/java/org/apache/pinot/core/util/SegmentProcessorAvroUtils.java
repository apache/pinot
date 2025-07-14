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

import com.google.auto.service.AutoService;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;


/**
 * Helper methods for avro related conversions needed, when using AVRO as intermediate format in segment processing.
 * AVRO is used as intermediate processing format in {@link SegmentProcessorFramework} and file-based impl of
 * {@link SegmentWriter}
 */
public final class SegmentProcessorAvroUtils {

  private static final EnumMap<FieldSpec.DataType, Schema> _notNullScalarMap;
  private static final EnumMap<FieldSpec.DataType, Schema> _nullScalarMap;
  private static final EnumMap<FieldSpec.DataType, Schema> _notNullMultiValueMap;
  private static final EnumMap<FieldSpec.DataType, Schema> _nullMultiValueMap;

  static {
    GenericData.get().addLogicalTypeConversion(new Conversions.BigDecimalConversion());

    _notNullScalarMap = new EnumMap<>(FieldSpec.DataType.class);
    _nullScalarMap = new EnumMap<>(FieldSpec.DataType.class);
    _notNullMultiValueMap = new EnumMap<>(FieldSpec.DataType.class);
    _nullMultiValueMap = new EnumMap<>(FieldSpec.DataType.class);

    Schema nullSchema = Schema.create(Schema.Type.NULL);
    for (DataType value : DataType.values()) {
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
          addType(value, Schema.create(Schema.Type.STRING), nullSchema);
          break;
        case BIG_DECIMAL:
          Schema bigDecimal = LogicalTypes.bigDecimal().addToSchema(SchemaBuilder.builder().bytesType());
          addType(value, bigDecimal, nullSchema);
          break;
        case BYTES:
          addType(value, Schema.create(Schema.Type.BYTES), nullSchema);
          break;
      }
    }
  }

  private static void addType(DataType dataType, Schema scalarSchema, Schema nullSchema) {
    _notNullScalarMap.put(dataType, scalarSchema);
    _nullScalarMap.put(dataType, Schema.createUnion(scalarSchema, nullSchema));
    Schema multiValueSchema = Schema.createArray(scalarSchema);
    _notNullMultiValueMap.put(dataType, multiValueSchema);
    _nullMultiValueMap.put(dataType, Schema.createUnion(multiValueSchema, nullSchema));
  }

  private SegmentProcessorAvroUtils() {
  }

  /**
   * Convert a GenericRow to an avro GenericRecord
   */
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord) {
    return convertGenericRowToAvroRecord(genericRow, reusableRecord, genericRow.getFieldToValueMap().keySet());
  }

  /**
   * Convert a GenericRow to an avro GenericRecord
   */
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord, Set<String> fields) {
    for (String field : fields) {
      Object value = genericRow.getValue(field);
      if (value instanceof Object[]) {
        reusableRecord.put(field, Arrays.asList((Object[]) value));
      } else {
        if (value instanceof byte[]) {
          value = ByteBuffer.wrap((byte[]) value);
        }
        reusableRecord.put(field, value);
      }
    }
    return reusableRecord;
  }

  /**
   * Converts a Pinot schema to an Avro schema
   */
  public static Schema convertPinotSchemaToAvroSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    List<FieldSpec> orderedFieldSpecs = pinotSchema.getAllFieldSpecs().stream()
        .sorted(Comparator.comparing(FieldSpec::getName))
        .collect(Collectors.toList());
    for (FieldSpec fieldSpec : orderedFieldSpecs) {
      String name = fieldSpec.getName();
      // TODO: Probably we shoudn't use stored type but define a correct avro conversion.
      //   See https://avro.apache.org/docs/1.11.0/spec.html#Logical+Types
      DataType storedType = fieldSpec.getDataType().getStoredType();

      Schema fieldType;
      if (fieldSpec.isSingleValueField()) {
        if (fieldSpec.isNullable()) {
          fieldType = _nullScalarMap.get(storedType);
        } else {
          fieldType = _notNullScalarMap.get(storedType);
        }
      } else {
        if (fieldSpec.isNullable()) {
          fieldType = _nullMultiValueMap.get(storedType);
        } else {
          fieldType = _notNullMultiValueMap.get(storedType);
        }
      }
      if (fieldType == null) {
        throw new RuntimeException("Unsupported data type: " + storedType);
      }

      fieldAssembler.name(name)
          .type(fieldType)
          .noDefault();
    }
    Schema schema = fieldAssembler.endRecord();
    return schema;
  }

  public static class BigDecimalPinotLogicalType extends LogicalType {
    public static final String NAME = "pinot.big_decimal";
    public BigDecimalPinotLogicalType() {
      super(NAME);
    }

    @Override
    public void validate(Schema schema) {
      if (schema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException("BigDecimal logical type can only be applied to STRING type");
      }
    }
  }
}
