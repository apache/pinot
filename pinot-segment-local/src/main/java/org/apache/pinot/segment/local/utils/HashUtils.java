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
   * For use-cases where the primary-key is set to columns that are guaranteed to be type-4 UUIDs, this hash-function
   * will reduce the number of bytes required from 36 to 16 for each UUID, without losing any precision. This leverages
   * the fact that a type-4 UUID is essentially a 16-byte value.
   */
  public static byte[] hashUUIDv4(byte[] bytes) {
    if (bytes.length % 36 != 0) {
      return bytes;
    }
    byte[] resultBytes = new byte[(bytes.length / 36) * 16];
    ByteBuffer byteBuffer = ByteBuffer.wrap(resultBytes).order(ByteOrder.BIG_ENDIAN);
    for (int chunk = 0; chunk < bytes.length; chunk += 36) {
      byte[] tempBytes = new byte[36];
      System.arraycopy(bytes, chunk, tempBytes, 0, tempBytes.length);
      UUID uuid;
      try {
        uuid = UUID.fromString(new String(tempBytes, StandardCharsets.UTF_8));
      } catch (Exception ignored) {
        // All bytes of the UUID in case of failures will be set to 0
        uuid = new UUID(0L, 0L);
      }
      long lsb = uuid.getLeastSignificantBits();
      long msb = uuid.getMostSignificantBits();
      byteBuffer.putLong(msb);
      byteBuffer.putLong(lsb);
    }
    return resultBytes;
  }

  public static Object hashPrimaryKey(PrimaryKey primaryKey, HashFunction hashFunction) {
    switch (hashFunction) {
      case NONE:
        return primaryKey;
      case MD5:
        return new ByteArray(HashUtils.hashMD5(primaryKey.asBytes()));
      case MURMUR3:
        return new ByteArray(HashUtils.hashMurmur3(primaryKey.asBytes()));
      case UUID_V4:
        return new ByteArray(HashUtils.hashUUIDv4(primaryKey.asBytes()));
      default:
        throw new IllegalArgumentException(String.format("Unrecognized hash function %s", hashFunction));
    }
  }
}
