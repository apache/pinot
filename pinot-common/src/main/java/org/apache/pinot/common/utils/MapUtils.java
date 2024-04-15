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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class MapUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapUtils.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MapUtils() {
  }

  public static byte[] serializeMap(Map<String, Object> map)
      throws JsonProcessingException {
    int size = map.size();

    // Directly return the size (0) for empty map
    if (size == 0) {
      return new byte[Integer.BYTES];
    }

    // Besides the value bytes, we store: size, length for each key, length for each value
    long bufferSize = (1 + 2 * (long) size) * Integer.BYTES;
    byte[][] keyBytesArray = new byte[size][];
    byte[][] valueBytesArray = new byte[size][];

    int index = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      byte[] keyBytes = entry.getKey().getBytes(UTF_8);
      bufferSize += keyBytes.length;
      keyBytesArray[index] = keyBytes;
      byte[] valueBytes = OBJECT_MAPPER.writeValueAsBytes(entry.getValue());
      bufferSize += valueBytes.length;
      valueBytesArray[index++] = valueBytes;
    }
    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(size);
    for (int i = 0; i < index; i++) {
      byte[] keyBytes = keyBytesArray[i];
      byteBuffer.putInt(keyBytes.length);
      byteBuffer.put(keyBytes);
      byte[] valueBytes = valueBytesArray[i];
      byteBuffer.putInt(valueBytes.length);
      byteBuffer.put(valueBytes);
    }
    return bytes;
  }

  public static Map<String, Object> deserializeMap(ByteBuffer byteBuffer) {
    int size = byteBuffer.getInt();
    if (size == 0) {
      return Map.of();
    }

    Map<String, Object> map = new java.util.HashMap<>(size);
    for (int i = 0; i < size; i++) {
      int keyLength = byteBuffer.getInt();
      byte[] keyBytes = new byte[keyLength];
      byteBuffer.get(keyBytes);
      String key = new String(keyBytes, UTF_8);
      int valueLength = byteBuffer.getInt();
      byte[] valueBytes = new byte[valueLength];
      byteBuffer.get(valueBytes);
      Object value = null;
      try {
        value = OBJECT_MAPPER.readValue(valueBytes, Object.class);
      } catch (IOException e) {
        LOGGER.error("Caught exception while deserializing value for key: {}", key, e);
      }
      map.put(key, value);
    }
    return map;
  }
}
