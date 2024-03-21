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
package org.apache.pinot.segment.local.utils;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


public class HashUtils {
  private HashUtils() {
  }

  public static byte[] hashMurmur3(byte[] bytes) {
    return Hashing.murmur3_128().hashBytes(bytes).asBytes();
  }

  public static byte[] hashMD5(byte[] bytes) {
    return Hashing.md5().hashBytes(bytes).asBytes();
  }

  /**
   * Returns a byte array that is a concatenation of the binary representation of each of the passed UUID values. This
   * is done by getting a String from each value by calling {@link Object#toString()}, which is then used to create a
   * {@link UUID} object. The 16 bytes of each UUID are appended to a buffer which is then returned in the result.
   * If any of the value is not a valid UUID, then this function appends the UTF-8 string bytes of all elements and
   * returns that in the result.
   */
  public static byte[] hashUUID(Object[] values) {
    byte[] result = new byte[values.length * 16];
    ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.BIG_ENDIAN);
    for (Object value : values) {
      if (value == null) {
        return concatenate(values);
      }
      String uuidString = value.toString();
      UUID uuid;
      try {
        uuid = UUID.fromString(uuidString);
      } catch (Throwable t) {
        return concatenate(values);
      }
      byteBuffer.putLong(uuid.getMostSignificantBits());
      byteBuffer.putLong(uuid.getLeastSignificantBits());
    }
    return result;
  }

  public static Object hashPrimaryKey(PrimaryKey primaryKey, HashFunction hashFunction) {
    switch (hashFunction) {
      case NONE:
        return primaryKey;
      case MD5:
        return new ByteArray(HashUtils.hashMD5(primaryKey.asBytes()));
      case MURMUR3:
        return new ByteArray(HashUtils.hashMurmur3(primaryKey.asBytes()));
      case UUID:
        return new ByteArray(HashUtils.hashUUID(primaryKey.getValues()));
      default:
        throw new IllegalArgumentException(String.format("Unrecognized hash function %s", hashFunction));
    }
  }

  private static byte[] concatenate(Object[] values) {
    byte[][] allValueBytes = new byte[values.length][];
    int totalLen = 0;
    for (int j = 0; j < allValueBytes.length; j++) {
      allValueBytes[j] = values[j] == null ? "null".getBytes(StandardCharsets.UTF_8)
          : values[j].toString().getBytes(StandardCharsets.UTF_8);
      totalLen += allValueBytes[j].length;
    }
    byte[] result = new byte[totalLen];
    for (int j = 0, offset = 0; j < allValueBytes.length; j++) {
      System.arraycopy(allValueBytes[j], 0, result, offset, allValueBytes[j].length);
      offset += allValueBytes[j].length;
    }
    return result;
  }
}
