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
import org.apache.avro.Schema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;


public class AvroSchemaUtil {
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
        return DataType.BYTES;
      default:
        throw new UnsupportedOperationException("Unsupported Avro type: " + avroType);
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
}
