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
package org.apache.pinot.segment.local.io.codec.transform;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import org.apache.pinot.segment.spi.codec.ChunkTransform;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Delta encoding transform for integer numeric values. Stores the first value as-is, then each
 * subsequent value as the difference from the previous value. Operates in-place on the
 * ByteBuffer.
 *
 * <p>Supports only INT and LONG stored types. Subtraction on IEEE 754 bit patterns does not
 * produce meaningful deltas, so FLOAT/DOUBLE columns should use {@link XorTransform} instead.</p>
 *
 * <p>Effective for monotonically increasing data (e.g., timestamps, counters).
 * Java two's complement arithmetic guarantees correct wrap-around for overflow.</p>
 */
public class DeltaTransform implements ChunkTransform {

  private static final Set<DataType> SUPPORTED_TYPES = EnumSet.of(DataType.INT, DataType.LONG);

  public static final DeltaTransform INSTANCE = new DeltaTransform();

  private DeltaTransform() {
  }

  @Override
  public Set<DataType> supportedTypes() {
    return SUPPORTED_TYPES;
  }

  @Override
  public void encode(ByteBuffer buffer, int numBytes, int valueSizeInBytes) {
    if (valueSizeInBytes == Integer.BYTES) {
      encodeInts(buffer, numBytes);
    } else {
      encodeLongs(buffer, numBytes);
    }
  }

  @Override
  public void decode(ByteBuffer buffer, int numBytes, int valueSizeInBytes) {
    if (valueSizeInBytes == Integer.BYTES) {
      decodeInts(buffer, numBytes);
    } else {
      decodeLongs(buffer, numBytes);
    }
  }

  private void encodeInts(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Integer.BYTES;
    if (numValues <= 1) {
      return;
    }
    int pos = buffer.position();
    // Encode backwards so each value can be read before being overwritten
    int prev = buffer.getInt(pos + (numValues - 2) * Integer.BYTES);
    for (int i = numValues - 1; i >= 1; i--) {
      int offset = pos + i * Integer.BYTES;
      int curr = buffer.getInt(offset);
      buffer.putInt(offset, curr - prev);
      if (i > 1) {
        prev = buffer.getInt(pos + (i - 2) * Integer.BYTES);
      }
    }
  }

  private void encodeLongs(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Long.BYTES;
    if (numValues <= 1) {
      return;
    }
    int pos = buffer.position();
    long prev = buffer.getLong(pos + (numValues - 2) * Long.BYTES);
    for (int i = numValues - 1; i >= 1; i--) {
      int offset = pos + i * Long.BYTES;
      long curr = buffer.getLong(offset);
      buffer.putLong(offset, curr - prev);
      if (i > 1) {
        prev = buffer.getLong(pos + (i - 2) * Long.BYTES);
      }
    }
  }

  private void decodeInts(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Integer.BYTES;
    if (numValues <= 1) {
      return;
    }
    int pos = buffer.position();
    for (int i = 1; i < numValues; i++) {
      int offset = pos + i * Integer.BYTES;
      int prevValue = buffer.getInt(pos + (i - 1) * Integer.BYTES);
      int delta = buffer.getInt(offset);
      buffer.putInt(offset, prevValue + delta);
    }
  }

  private void decodeLongs(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Long.BYTES;
    if (numValues <= 1) {
      return;
    }
    int pos = buffer.position();
    for (int i = 1; i < numValues; i++) {
      int offset = pos + i * Long.BYTES;
      long prevValue = buffer.getLong(pos + (i - 1) * Long.BYTES);
      long delta = buffer.getLong(offset);
      buffer.putLong(offset, prevValue + delta);
    }
  }
}
