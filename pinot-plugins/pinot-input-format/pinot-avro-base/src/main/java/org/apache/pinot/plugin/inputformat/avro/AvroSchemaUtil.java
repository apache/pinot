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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;


public class AvroSchemaUtil {
  /*
   * These constants are copied from org.apache.avro.LogicalTypes
   */
  private static final String DECIMAL = "decimal";
  private static final String UUID = "uuid";
  private static final String DATE = "date";
  private static final String TIME_MILLIS = "time-millis";
  private static final String TIME_MICROS = "time-micros";
  private static final String TIMESTAMP_MILLIS = "timestamp-millis";
  private static final String TIMESTAMP_MICROS = "timestamp-micros";
  private static final Map<String, Conversion<?>> CONVERSION_MAP = new HashMap<>();

  static {
    CONVERSION_MAP.put(DECIMAL, new Conversions.DecimalConversion());
    CONVERSION_MAP.put(UUID, new Conversions.UUIDConversion());
    CONVERSION_MAP.put(DATE, new TimeConversions.DateConversion());
    CONVERSION_MAP.put(TIME_MILLIS, new TimeConversions.TimeMillisConversion());
    CONVERSION_MAP.put(TIME_MICROS, new TimeConversions.TimeMicrosConversion());
    CONVERSION_MAP.put(TIMESTAMP_MILLIS, new TimeConversions.TimestampMillisConversion());
    CONVERSION_MAP.put(TIMESTAMP_MICROS, new TimeConversions.TimestampMicrosConversion());
  }

  private AvroSchemaUtil() {
  }

  public static Conversion<?> findConversionFor(String typeName) {
    return CONVERSION_MAP.get(typeName);
  }

  /**
   * Returns the data type stored in Pinot that is associated with the given Avro type.
   */
  public static DataType valueOf(Schema.Type avroType) {
    switch (avroType) {
      case INT:
        return DataType.INT;
      case LONG:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case STRING:
      case ENUM:
        return DataType.STRING;
      case BYTES:
      case FIXED:
        return DataType.BYTES;
      case MAP:
      case ARRAY:
      case RECORD:
      case UNION:
        return DataType.JSON;
      default:
        throw new UnsupportedOperationException("Unsupported Avro type: " + avroType);
    }
  }

  /**
   * @return if the given avro type is a primitive type.
   */
  public static boolean isPrimitiveType(Schema.Type avroType) {
    switch (avroType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case STRING:
      case ENUM:
        return true;
      default:
        return false;
    }
  }

  public static ObjectNode toAvroSchemaJsonObject(FieldSpec fieldSpec) {
    ObjectNode jsonSchema = JsonUtils.newObjectNode();
    jsonSchema.put("name", fieldSpec.getName());
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "int"));
        return jsonSchema;
      case LONG:
        jsonSchema.set("type", convertStringsToJsonArray("null", "long"));
        return jsonSchema;
      case FLOAT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "float"));
        return jsonSchema;
      case DOUBLE:
        jsonSchema.set("type", convertStringsToJsonArray("null", "double"));
        return jsonSchema;
      case STRING:
      case JSON:
        jsonSchema.set("type", convertStringsToJsonArray("null", "string"));
        return jsonSchema;
      case BYTES:
        jsonSchema.set("type", convertStringsToJsonArray("null", "bytes"));
        return jsonSchema;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static ArrayNode convertStringsToJsonArray(String... strings) {
    ArrayNode jsonArray = JsonUtils.newArrayNode();
    for (String string : strings) {
      jsonArray.add(string);
    }
    return jsonArray;
  }

  /**
   * Applies the logical type conversion to the given Avro record field. If there isn't a logical
   * type for the value then the value is returned unchanged. If there is a logical type associated
   * to the field but no Avro conversion is known for the type then the value is returned unchanged.
   *
   * @param field Avro field spec
   * @param value Value of the field
   * @return Converted value as per the logical type in the spec, or the unchanged value if a
   *     logical type or conversion can't be found.
   */
  public static Object applyLogicalType(Schema.Field field, Object value) {
    if (field == null || field.schema() == null) {
      return value;
    }
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(field.schema());
    if (logicalType == null) {
      return value;
    }
    Conversion<?> conversion = AvroSchemaUtil.findConversionFor(logicalType.getName());
    if (conversion == null) {
      return value;
    }
    return Conversions.convertToLogicalType(value, field.schema(), logicalType, conversion);
  }
}
