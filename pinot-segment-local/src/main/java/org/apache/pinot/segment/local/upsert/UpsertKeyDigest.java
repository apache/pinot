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
package org.apache.pinot.segment.local.upsert;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


public final class UpsertKeyDigest {
  public static final int BUCKETS = 256;
  public static final String ALGORITHM = "XXH64-KEY-CV-XOR-256-V1";
  public static final String ZERO_TOTAL = "0000000000000000";
  private static final XXHash64 XXH64 = XXHashFactory.fastestInstance().hash64();
  private static final long KEY_SEED = 0x9E3779B97F4A7C15L;
  private static final long VALUE_SEED = 0xC2B2AE3D27D4EB4FL;
  private static final long TAG_NULL = 0;
  private static final long TAG_INT = 1;
  private static final long TAG_LONG = 2;
  private static final long TAG_FLOAT = 3;
  private static final long TAG_DOUBLE = 4;
  private static final long TAG_BIG_DECIMAL = 5;
  private static final long TAG_STRING = 6;
  private static final long TAG_BYTES = 7;
  private static final long TAG_COLUMNS = 8;
  private static final long TAG_OTHER = 9;

  private final AtomicLongArray _buckets = new AtomicLongArray(BUCKETS);
  private final LongAdder _entries = new LongAdder();
  // Seqlock: segment-level operations bump the epoch on entry and exit and hold the depth in between.
  private final AtomicLong _epoch = new AtomicLong();
  private final AtomicInteger _depth = new AtomicInteger();

  /// Adds one entry. A tombstone (delete record) contributes nothing.
  public void add(Object storedKey, @Nullable Comparable comparisonValue, boolean deleteRecord) {
    if (deleteRecord) {
      return;
    }
    long keyHash = hashKey(storedKey);
    xor(keyHash, entryHash(keyHash, comparisonValue));
    _entries.increment();
  }

  /// Removes one entry that was added with the same key, comparison value and tombstone flag.
  public void remove(Object storedKey, @Nullable Comparable comparisonValue, boolean deleteRecord) {
    if (deleteRecord) {
      return;
    }
    long keyHash = hashKey(storedKey);
    xor(keyHash, entryHash(keyHash, comparisonValue));
    _entries.decrement();
  }

  /// Replaces the entry of one key. Equal contributions cancel without touching the buckets.
  public void update(Object storedKey, @Nullable Comparable oldComparisonValue, boolean oldDeleteRecord,
      @Nullable Comparable newComparisonValue, boolean newDeleteRecord) {
    if (oldDeleteRecord && newDeleteRecord) {
      return;
    }
    long keyHash = hashKey(storedKey);
    long oldHash = oldDeleteRecord ? 0 : entryHash(keyHash, oldComparisonValue);
    long newHash = newDeleteRecord ? 0 : entryHash(keyHash, newComparisonValue);
    if (oldHash != newHash) {
      xor(keyHash, oldHash ^ newHash);
    }
    if (oldDeleteRecord) {
      _entries.increment();
    } else if (newDeleteRecord) {
      _entries.decrement();
    }
  }

  /// Marks the start of a segment-level operation (add, preload, replace, remove) on a non-consumer thread.
  public void beginUnstable() {
    _depth.incrementAndGet();
    _epoch.incrementAndGet();
  }

  public void endUnstable() {
    _epoch.incrementAndGet();
    _depth.decrementAndGet();
  }

  /// Copies the buckets without blocking writers. The mark is stable only if no segment-level operation was in
  /// flight at either read and none started in between.
  public Mark freeze() {
    long epochBefore = _epoch.get();
    int depthBefore = _depth.get();
    long[] buckets = new long[BUCKETS];
    long total = 0;
    for (int i = 0; i < BUCKETS; i++) {
      buckets[i] = _buckets.get(i);
      total ^= buckets[i];
    }
    long entries = _entries.sum();
    int depthAfter = _depth.get();
    long epochAfter = _epoch.get();
    return new Mark(buckets, total, entries, depthBefore == 0 && depthAfter == 0 && epochBefore == epochAfter);
  }

  private void xor(long keyHash, long contribution) {
    int bucket = (int) (keyHash >>> 56);
    _buckets.accumulateAndGet(bucket, contribution, (a, b) -> a ^ b);
  }

  /// Hashes the map key as stored: the raw bytes of a hashed key, or the serialized values of a plain PrimaryKey.
  public static long hashKey(Object storedKey) {
    if (storedKey instanceof ByteArray byteArray) {
      return hashBytes(byteArray.getBytes(), KEY_SEED);
    }
    if (storedKey instanceof PrimaryKey primaryKey) {
      return hashBytes(primaryKey.asBytes(), KEY_SEED);
    }
    return hashBytes(String.valueOf(storedKey).getBytes(StandardCharsets.UTF_8), KEY_SEED ^ TAG_OTHER);
  }

  /// Hashes the comparison value through its stored type, never through toString for the known types.
  public static long entryHash(long keyHash, @Nullable Comparable comparisonValue) {
    return mix64(keyHash * KEY_SEED ^ hashComparisonValue(comparisonValue));
  }

  private static long hashComparisonValue(@Nullable Comparable value) {
    if (value == null) {
      return mix64(TAG_NULL);
    }
    if (value instanceof Integer intValue) {
      return tagged(TAG_INT, intValue);
    }
    if (value instanceof Long longValue) {
      return tagged(TAG_LONG, longValue);
    }
    if (value instanceof Float floatValue) {
      return tagged(TAG_FLOAT, Float.floatToIntBits(floatValue));
    }
    if (value instanceof Double doubleValue) {
      return tagged(TAG_DOUBLE, Double.doubleToLongBits(doubleValue));
    }
    if (value instanceof String string) {
      return tagged(TAG_STRING, hashBytes(string.getBytes(StandardCharsets.UTF_8), VALUE_SEED));
    }
    if (value instanceof ByteArray byteArray) {
      return tagged(TAG_BYTES, hashBytes(byteArray.getBytes(), VALUE_SEED));
    }
    if (value instanceof BigDecimal bigDecimal) {
      return tagged(TAG_BIG_DECIMAL, hashBytes(BigDecimalUtils.serialize(bigDecimal), VALUE_SEED));
    }
    if (value instanceof ComparisonColumns columns) {
      Comparable[] values = columns.getValues();
      long hash = tagged(TAG_COLUMNS, values.length);
      for (int i = 0; i < values.length; i++) {
        hash = mix64(hash + i * VALUE_SEED ^ hashComparisonValue(values[i]));
      }
      return hash;
    }
    return tagged(TAG_OTHER, hashBytes(value.toString().getBytes(StandardCharsets.UTF_8), VALUE_SEED));
  }

  private static long tagged(long tag, long bits) {
    return mix64(tag * VALUE_SEED ^ bits);
  }

  private static long hashBytes(byte[] bytes, long seed) {
    return XXH64.hash(bytes, 0, bytes.length, seed);
  }

  /// SplitMix64 finalizer: cheap, allocation free, and deterministic across JVMs.
  private static long mix64(long z) {
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  public static String toHex(long value) {
    return HexFormat.of().toHexDigits(value);
  }

  /// Big-endian longs, base64, 2 KB before encoding.
  public static String encodeBuckets(long[] buckets) {
    ByteBuffer buffer = ByteBuffer.allocate(buckets.length * Long.BYTES);
    for (long bucket : buckets) {
      buffer.putLong(bucket);
    }
    return Base64.getEncoder().encodeToString(buffer.array());
  }

  public static long[] decodeBuckets(String encoded) {
    ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encoded));
    long[] buckets = new long[buffer.remaining() / Long.BYTES];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = buffer.getLong();
    }
    return buckets;
  }

  public record Mark(long[] buckets, long total, long entries, boolean stable) {
    public boolean sameEntries(Mark other) {
      return total == other.total && entries == other.entries && Arrays.equals(buckets, other.buckets);
    }
  }
}
