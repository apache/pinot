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
package org.apache.pinot.spi.utils;

import java.util.UUID;


/// Immutable UUID key optimized for Pinot execution hot paths.
///
/// The key stores UUID values as two primitive longs to avoid repeated [ByteArray] allocation and byte-by-byte
/// equality/hashing while preserving Pinot's canonical 16-byte representation at the edges.
public final class UuidKey implements Comparable<UuidKey> {
  private final long _mostSignificantBits;
  private final long _leastSignificantBits;
  private final int _hashCode;

  private UuidKey(long mostSignificantBits, long leastSignificantBits) {
    _mostSignificantBits = mostSignificantBits;
    _leastSignificantBits = leastSignificantBits;
    _hashCode = UuidUtils.hashCode(mostSignificantBits, leastSignificantBits);
  }

  public static UuidKey fromLongs(long mostSignificantBits, long leastSignificantBits) {
    return new UuidKey(mostSignificantBits, leastSignificantBits);
  }

  public static UuidKey fromBytes(byte[] uuidBytes) {
    return fromLongs(UuidUtils.getMostSignificantBits(uuidBytes), UuidUtils.getLeastSignificantBits(uuidBytes));
  }

  public static UuidKey fromByteArray(ByteArray uuidBytes) {
    return fromBytes(uuidBytes.getBytes());
  }

  public static UuidKey fromUUID(UUID uuid) {
    return fromLongs(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public static UuidKey fromObject(Object value) {
    if (value instanceof UuidKey) {
      return (UuidKey) value;
    }
    if (value instanceof UUID) {
      return fromUUID((UUID) value);
    }
    if (value instanceof byte[]) {
      return fromBytes((byte[]) value);
    }
    if (value instanceof ByteArray) {
      return fromByteArray((ByteArray) value);
    }
    if (value instanceof CharSequence) {
      return fromUUID(UuidUtils.toUUID(value.toString()));
    }
    throw new IllegalArgumentException(
        "Cannot convert value: '" + value + "' to UUID key, unsupported type: " + value.getClass());
  }

  public long getMostSignificantBits() {
    return _mostSignificantBits;
  }

  public long getLeastSignificantBits() {
    return _leastSignificantBits;
  }

  public byte[] toBytes() {
    return UuidUtils.toBytes(_mostSignificantBits, _leastSignificantBits);
  }

  public ByteArray toByteArray() {
    return new ByteArray(toBytes());
  }

  public UUID toUUID() {
    return UuidUtils.toUUID(_mostSignificantBits, _leastSignificantBits);
  }

  @Override
  public int compareTo(UuidKey other) {
    return UuidUtils.compare(_mostSignificantBits, _leastSignificantBits, other._mostSignificantBits,
        other._leastSignificantBits);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof UuidKey)) {
      return false;
    }
    UuidKey otherUuidKey = (UuidKey) other;
    return UuidUtils.equals(_mostSignificantBits, _leastSignificantBits, otherUuidKey._mostSignificantBits,
        otherUuidKey._leastSignificantBits);
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }

  @Override
  public String toString() {
    return toUUID().toString();
  }
}
