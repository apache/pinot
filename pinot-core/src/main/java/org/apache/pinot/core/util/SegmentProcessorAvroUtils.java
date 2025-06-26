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
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
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
      DataType storedType = fieldSpec.getDataType().getStoredType();

      SchemaBuilder.BaseFieldTypeBuilder<Schema> type;
      if (fieldSpec.isNullable()) {
        type = fieldAssembler.name(name).type().nullable();
      } else {
        type = fieldAssembler.name(name).type();
      }

      String logicalType = "pinot." + fieldSpec.getDataType().toString().toLowerCase(Locale.US);
      if (fieldSpec.isSingleValueField()) {
        switch (storedType) {
          case INT:
            fieldAssembler = type.intBuilder()
                .prop("logicalType", logicalType)
                .endInt()
                .noDefault();
            break;
          case LONG:
            fieldAssembler = type.longBuilder()
                .prop("logicalType", logicalType)
                .endLong()
                .noDefault();
            break;
          case FLOAT:
            fieldAssembler = type.floatBuilder()
                .prop("logicalType", logicalType)
                .endFloat()
                .noDefault();
            break;
          case DOUBLE:
            fieldAssembler = type.doubleBuilder()
                .prop("logicalType", logicalType)
                .endDouble()
                .noDefault();
            break;
          case STRING:
          case BIG_DECIMAL:
            fieldAssembler = type.stringBuilder()
                .prop("logicalType", logicalType)
                .endString()
                .noDefault();
            break;
          case BYTES:
            fieldAssembler = type.bytesBuilder()
                .prop("logicalType", logicalType)
                .endBytes()
                .noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      } else {
        SchemaBuilder.TypeBuilder<SchemaBuilder.ArrayDefault<Schema>> arrayBuilder = type.array().items();
        switch (storedType) {
          case INT:
            fieldAssembler = arrayBuilder.intBuilder()
                .prop("logicalType", logicalType)
                .endInt()
                .noDefault();
            break;
          case LONG:
            fieldAssembler = arrayBuilder.longBuilder()
                .prop("logicalType", logicalType)
                .endLong()
                .noDefault();
            break;
          case FLOAT:
            fieldAssembler = arrayBuilder.floatBuilder()
                .prop("logicalType", logicalType)
                .endFloat()
                .noDefault();
            break;
          case DOUBLE:
            fieldAssembler = arrayBuilder.doubleBuilder()
                .prop("logicalType", logicalType)
                .endDouble()
                .noDefault();
            break;
          case STRING:
          case BIG_DECIMAL:
            fieldAssembler = arrayBuilder.stringBuilder()
                .prop("logicalType", logicalType)
                .endString()
                .noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      }
    }
    return fieldAssembler.endRecord();
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

  @AutoService(Conversion.class)
  public static class BigDecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return BigDecimalPinotLogicalType.NAME;
    }

    @Override
    public BigDecimal fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return new BigDecimal(value.toString());
    }

    @Override
    public CharSequence toCharSequence(BigDecimal value, Schema schema, LogicalType type) {
      return value.toString();
    }
  }
}
