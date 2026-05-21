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
import org.apache.pinot.spi.utils.UuidUtils;


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
    Schema avroSchema = reusableRecord.getSchema();
    for (String field : fields) {
      Object value = genericRow.getValue(field);
      if (value instanceof Object[]) {
        Schema.Field avroField = avroSchema.getField(field);
        if (avroField != null && isUuidArrayLogicalType(avroField.schema())) {
          // MV UUID columns are emitted with an Avro array<string{logicalType:uuid}> schema; convert each
          // 16-byte element to its canonical UUID string so GenericDatumWriter accepts the record.
          Object[] elements = (Object[]) value;
          List<Object> converted = new ArrayList<>(elements.length);
          for (Object element : elements) {
            converted.add(element instanceof byte[] ? UuidUtils.toString((byte[]) element) : element);
          }
          reusableRecord.put(field, converted);
        } else {
          reusableRecord.put(field, Arrays.asList((Object[]) value));
        }
      } else {
        if (value instanceof byte[]) {
          // UUID columns are emitted with an Avro string{logicalType:uuid} schema (Avro 1.x only allows the
          // uuid logical type on string), so the 16-byte canonical form must be rendered as a canonical
          // UUID string here. Plain BYTES columns continue to be wrapped as ByteBuffer.
          Schema.Field avroField = avroSchema.getField(field);
          if (avroField != null && isUuidLogicalType(avroField.schema())) {
            value = UuidUtils.toString((byte[]) value);
          } else {
            value = ByteBuffer.wrap((byte[]) value);
          }
        }
        reusableRecord.put(field, value);
      }
    }
    return reusableRecord;
  }

  private static boolean isUuidLogicalType(Schema schema) {
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    return logicalType != null && "uuid".equals(logicalType.getName());
  }

  private static boolean isUuidArrayLogicalType(Schema schema) {
    return schema.getType() == Schema.Type.ARRAY && isUuidLogicalType(schema.getElementType());
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
