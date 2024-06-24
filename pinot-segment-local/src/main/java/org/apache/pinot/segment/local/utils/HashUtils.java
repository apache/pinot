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
   * Returns a byte array that is a concatenation of the binary representation of each of the passed UUID values.
   * If any of the values is not a valid UUID, then we return the result of {@link PrimaryKey#asBytes()}.
   */
  public static byte[] hashUUID(PrimaryKey primaryKey) {
    Object[] values = primaryKey.getValues();
    byte[] result = new byte[values.length * 16];
    ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.BIG_ENDIAN);
    for (Object value : values) {
      if (value == null) {
        throw new IllegalArgumentException("Found null value in primary key");
      }
      UUID uuid;
      try {
        uuid = UUID.fromString(value.toString());
      } catch (Throwable t) {
        return primaryKey.asBytes();
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
        return new ByteArray(HashUtils.hashUUID(primaryKey));
      default:
        throw new IllegalArgumentException(String.format("Unrecognized hash function %s", hashFunction));
    }
  }
}
