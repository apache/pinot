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
package org.apache.pinot.common.response.encoder;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;


public class JsonResponseEncoder implements ResponseEncoder {

  @Override
  public byte[] encodeResultTable(ResultTable resultTable, int startRow, int length)
      throws IOException {
    // Serialize the rows to a byte array
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (int j = startRow; j < Math.min(startRow + length, resultTable.getRows().size()); j++) {
      String rowString = JsonUtils.objectToJsonNode(resultTable.getRows().get(j)).toString();
      byte[] bytesToWrite = rowString.getBytes(StandardCharsets.UTF_8);
      byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(bytesToWrite.length).array());
      byteArrayOutputStream.write(bytesToWrite);
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  public ResultTable decodeResultTable(byte[] bytes, int rowSize, DataSchema schema)
      throws IOException {
    List<Object[]> rows = new ArrayList<>();
    int bytesRead = 0;
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    for (int i = 0; i < rowSize; i++) {
      int nextRowSize = byteBuffer.getInt(bytesRead);
      bytesRead += 4;
      byte[] rowBytes = new byte[nextRowSize];
      byteBuffer.position(bytesRead);
      byteBuffer.get(rowBytes);
      bytesRead += nextRowSize;
      String rowString = new String(rowBytes);
      JsonNode jsonRow = JsonUtils.stringToJsonNode(rowString);
      Object[] row = new Object[jsonRow.size()];
      for (int columnIdx = 0; columnIdx < jsonRow.size(); columnIdx++) {
        DataSchema.ColumnDataType columnDataType = schema.getColumnDataType(columnIdx);
        JsonNode jsonValue = jsonRow.get(columnIdx);
        if (columnDataType.isArray()) {
          row[columnIdx] = extractArray(columnDataType, jsonValue);
        } else if (columnDataType == DataSchema.ColumnDataType.MAP) {
          row[columnIdx] = extractMap(jsonValue);
        } else {
          row[columnIdx] = extractValue(columnDataType, jsonValue);
        }
      }
      rows.add(row);
    }
    return new ResultTable(schema, rows);
  }

  private Object[] extractArray(JsonNode jsonValue) {
    Object[] array = new Object[jsonValue.size()];
    for (int k = 0; k < jsonValue.size(); k++) {
      if (jsonValue.get(k).isNull()) {
        array[k] = null;
      } else if (jsonValue.get(k).isBoolean()) {
        array[k] = jsonValue.get(k).asBoolean();
      } else if (jsonValue.get(k).isInt()) {
        array[k] = jsonValue.get(k).asInt();
      } else if (jsonValue.get(k).isLong()) {
        array[k] = jsonValue.get(k).asLong();
      } else if (jsonValue.get(k).isFloat()) {
        array[k] = jsonValue.get(k).floatValue();
      } else if (jsonValue.get(k).isDouble()) {
        array[k] = jsonValue.get(k).asDouble();
      } else if (jsonValue.get(k).isTextual()) {
        array[k] = jsonValue.get(k).textValue();
      } else if (jsonValue.isArray()) {
        array[k] = extractArray(jsonValue.get(k));
      } else if (jsonValue.isObject()) {
        array[k] = extractMap(jsonValue.get(k));
      } else {
        array[k] = jsonValue.get(k).toString();
      }
    }
    return array;
  }

  private Object extractValue(JsonNode jsonValue) {
    if (jsonValue.isNull()) {
      return null;
    }
    if (jsonValue.isBoolean()) {
      return jsonValue.asBoolean();
    }
    if (jsonValue.isShort()) {
      return jsonValue.shortValue();
    }
    if (jsonValue.isBigInteger()) {
      return jsonValue.bigIntegerValue();
    }
    if (jsonValue.isBigDecimal()) {
      return jsonValue.decimalValue();
    }
    if (jsonValue.isInt()) {
      return jsonValue.asInt();
    }
    if (jsonValue.isLong()) {
      return jsonValue.asLong();
    }
    if (jsonValue.isFloat()) {
      return jsonValue.floatValue();
    }
    if (jsonValue.isDouble()) {
      return jsonValue.asDouble();
    }
    if (jsonValue.isTextual()) {
      return jsonValue.textValue();
    }
    if (jsonValue.isArray()) {
      return extractArray(jsonValue);
    }
    if (jsonValue.isObject()) {
      return extractMap(jsonValue);
    }
    return jsonValue.toString();
  }

  private Map<String, Object> extractMap(JsonNode jsonValue) {
    Map<String, Object> map = new HashMap<>();
    jsonValue.fields().forEachRemaining(entry -> {
      String key = entry.getKey();
      Object value = extractValue(entry.getValue());
      map.put(key, value);
    });
    return map;
  }

  private static Object extractArray(DataSchema.ColumnDataType columnDataType, JsonNode jsonValue) {
    switch (columnDataType) {
      case BOOLEAN_ARRAY:
        boolean[] booleanArray = new boolean[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          booleanArray[k] = jsonValue.get(k).asBoolean();
        }
        return booleanArray;
      case INT_ARRAY:
        int[] intArray = new int[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          intArray[k] = jsonValue.get(k).asInt();
        }
        return intArray;
      case LONG_ARRAY:
        long[] longArray = new long[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          longArray[k] = jsonValue.get(k).asLong();
        }
        return longArray;
      case FLOAT_ARRAY:
        float[] floatArray = new float[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          floatArray[k] = jsonValue.get(k).floatValue();
        }
        return floatArray;
      case DOUBLE_ARRAY:
        double[] doubleArray = new double[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          doubleArray[k] = jsonValue.get(k).asDouble();
        }
        return doubleArray;
      case STRING_ARRAY:
      case TIMESTAMP_ARRAY:
      case BYTES_ARRAY:
        String[] stringArray = new String[jsonValue.size()];
        for (int k = 0; k < jsonValue.size(); k++) {
          stringArray[k] = jsonValue.get(k).textValue();
        }
        return stringArray;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + columnDataType);
    }
  }

  private static Object extractValue(DataSchema.ColumnDataType columnDataType, JsonNode jsonValue)
      throws IOException {
    if (jsonValue.isNull()) {
      return null;
    }
    switch (columnDataType) {
      case BOOLEAN:
        return jsonValue.asBoolean();
      case INT:
        return jsonValue.asInt();
      case LONG:
        return jsonValue.asLong();
      case FLOAT:
        return Double.valueOf(jsonValue.asDouble()).floatValue();
      case DOUBLE:
        return jsonValue.asDouble();
      case STRING:
      case BYTES:
      case TIMESTAMP:
      case JSON:
      case BIG_DECIMAL:
      case OBJECT:
        return jsonValue.textValue();
      case UNKNOWN:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + columnDataType);
    }
  }
}
