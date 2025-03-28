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
import java.util.List;
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
          row[columnIdx] = extractArray(jsonValue);
        } else {
          row[columnIdx] = extractObject(jsonValue);
        }
      }
      rows.add(row);
    }
    return new ResultTable(schema, rows);
  }

  private Object[] extractArray(JsonNode jsonValue) {
    Object[] array = new Object[jsonValue.size()];
    for (int k = 0; k < jsonValue.size(); k++) {
      if (jsonValue.get(k).isInt()) {
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
        array[k] = extractObject(jsonValue.get(k));
      } else {
        array[k] = jsonValue.get(k).toString();
      }
    }
    return array;
  }

  private Object extractObject(JsonNode jsonValue) {
    if (jsonValue.isInt()) {
      return jsonValue.asInt();
    } else if (jsonValue.isLong()) {
      return jsonValue.asLong();
    } else if (jsonValue.isFloat()) {
      return jsonValue.floatValue();
    } else if (jsonValue.isDouble()) {
      return jsonValue.asDouble();
    } else if (jsonValue.isTextual()) {
      return jsonValue.textValue();
    } else if (jsonValue.isArray()) {
      return extractArray(jsonValue);
    } else if (jsonValue.isObject()) {
      return extractObject(jsonValue);
    } else {
      return jsonValue.toString();
    }
  }
}
