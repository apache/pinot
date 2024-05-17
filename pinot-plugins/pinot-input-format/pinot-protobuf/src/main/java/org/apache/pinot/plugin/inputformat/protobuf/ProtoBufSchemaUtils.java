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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;

public class ProtoBufSchemaUtils {

  private ProtoBufSchemaUtils() {
  }

  /**
   * Given an Protobuf schema, flatten/unnest the complex types based on the config, and then map from column to
   * field type and time unit, return the equivalent Pinot schema.
   *
   * @param protoSchema Avro schema
   * @param fieldTypeMap Map from column to field type
   * @param timeUnit Time unit
   * @param fieldsToUnnest the fields to unnest
   * @param delimiter the delimiter to separate components in nested structure
   *
   * @return Pinot schema
   */
  public static Schema getPinotSchemaFromPinotSchemaWithComplexTypeHandling(Descriptors.Descriptor protoSchema,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit, List<String> fieldsToUnnest,
      String delimiter) {
    Schema pinotSchema = new Schema();

    for (Descriptors.FieldDescriptor field : protoSchema.getFields()) {
      extractSchemaWithComplexTypeHandling(field, fieldsToUnnest, delimiter, field.getName(), pinotSchema,
          fieldTypeMap, timeUnit);
    }
    return pinotSchema;
  }

  static void extractSchemaWithComplexTypeHandling(
      Descriptors.FieldDescriptor fieldSchema,
      List<String> fieldsToUnnest,
      String delimiter,
      String path,
      Schema pinotSchema,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap,
      @Nullable TimeUnit timeUnit) {
    Descriptors.FieldDescriptor.Type fieldType = fieldSchema.getType();
    if (fieldSchema.isRepeated()) {
      if (isPrimitiveType(fieldType)) {
        addFieldToPinotSchema(pinotSchema, valueOf(fieldType), path, false, fieldTypeMap, timeUnit);
      } else if (fieldsToUnnest.contains(path) && !fieldSchema.isMapField()) {
        for (Descriptors.FieldDescriptor innerField : fieldSchema.getMessageType().getFields()) {
          extractSchemaWithComplexTypeHandling(innerField, fieldsToUnnest, delimiter,
              String.join(delimiter, path, innerField.getName()), pinotSchema, fieldTypeMap, timeUnit);
        }
      } else if (!fieldSchema.isMapField()) {
        addFieldToPinotSchema(pinotSchema, FieldSpec.DataType.STRING, path, true, fieldTypeMap, timeUnit);
      }
      // Ignores Map type since it's not supported when complex type handling is enabled
    } else if (fieldType == Descriptors.FieldDescriptor.Type.MESSAGE) {
      for (Descriptors.FieldDescriptor innerField : fieldSchema.getMessageType().getFields()) {
        extractSchemaWithComplexTypeHandling(innerField, fieldsToUnnest, delimiter,
            String.join(delimiter, path, innerField.getName()), pinotSchema, fieldTypeMap, timeUnit);
      }
    } else {
      FieldSpec.DataType dataType = valueOf(fieldType);
      addFieldToPinotSchema(pinotSchema, dataType, path, true, fieldTypeMap, timeUnit);
    }
  }

  public static FieldSpec.DataType valueOf(Descriptors.FieldDescriptor.Type pinotType) {
    switch (pinotType) {
      case INT32:
      case UINT32:
      case SINT32:
      case FIXED32:
      case SFIXED32:
        return FieldSpec.DataType.INT;
      case INT64:
      case UINT64:
      case FIXED64:
      case SINT64:
      case SFIXED64:
        return FieldSpec.DataType.LONG;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case BOOL:
        return FieldSpec.DataType.BOOLEAN;
      case BYTES:
        return FieldSpec.DataType.BYTES;
      case STRING:
      case ENUM:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException("Unsupported ProtoBuf type: " + pinotType);
    }
  }

  /**
   * @return if the given avro type is a primitive type.
   */
  public static boolean isPrimitiveType(Descriptors.FieldDescriptor.Type pinotType) {
    switch (pinotType) {
      case INT32:
      case INT64:
      case UINT32:
      case UINT64:
      case SINT32:
      case SINT64:
      case FIXED64:
      case FIXED32:
      case SFIXED64:
      case DOUBLE:
      case FLOAT:
      case BOOL:
      case BYTES:
      case STRING:
      case ENUM:
        return true;
      default:
        return false;
    }
  }

  private static void addFieldToPinotSchema(Schema pinotSchema, FieldSpec.DataType dataType, String name,
      boolean isSingleValueField, @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap,
      @Nullable TimeUnit timeUnit) {
    if (fieldTypeMap == null) {
      pinotSchema.addField(new DimensionFieldSpec(name, dataType, isSingleValueField));
    } else {
      FieldSpec.FieldType fieldType = fieldTypeMap.getOrDefault(name, FieldSpec.FieldType.DIMENSION);
      Preconditions.checkNotNull(fieldType, "Field type not specified for field: %s", name);
      switch (fieldType) {
        case DIMENSION:
          pinotSchema.addField(new DimensionFieldSpec(name, dataType, isSingleValueField));
          break;
        case METRIC:
          Preconditions.checkState(isSingleValueField, "Metric field: %s cannot be multi-valued", name);
          pinotSchema.addField(new MetricFieldSpec(name, dataType));
          break;
        case DATE_TIME:
          Preconditions.checkState(isSingleValueField, "Time field: %s cannot be multi-valued", name);
          Preconditions.checkNotNull(timeUnit, "Time unit cannot be null");
          // TODO: Switch to new format after releasing 0.11.0
          //       "EPOCH|" + timeUnit.name()
          String format = "1:" + timeUnit.name() + ":EPOCH";
          String granularity = "1:" + timeUnit.name();
          pinotSchema.addField(new DateTimeFieldSpec(name, dataType, format, granularity));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType + " for field: " + name);
      }
    }
  }
}
