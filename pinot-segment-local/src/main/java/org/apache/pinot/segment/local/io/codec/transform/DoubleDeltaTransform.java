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
 * Double-delta (delta-of-delta) transform for integer numeric values. Stores the first value
 * as-is, the first delta as-is, then each subsequent value as the difference between
 * consecutive deltas. Operates in-place on the ByteBuffer.
 *
 * <p>Supports only INT and LONG stored types. Subtraction on IEEE 754 bit patterns does not
 * produce meaningful double-deltas, so FLOAT/DOUBLE columns should use {@link XorTransform}
 * instead.</p>
 *
 * <p>Effective for data with constant or near-constant step sizes (e.g., fixed-interval
 * timestamps).</p>
 */
public class DoubleDeltaTransform implements ChunkTransform {

  private static final Set<DataType> SUPPORTED_TYPES = EnumSet.of(DataType.INT, DataType.LONG);

  public static final DoubleDeltaTransform INSTANCE = new DoubleDeltaTransform();

  private DoubleDeltaTransform() {
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
    if (numValues <= 2) {
      // For 0, 1, or 2 values, just apply regular delta (first value stays, second becomes delta)
      if (numValues == 2) {
        int pos = buffer.position();
        int v0 = buffer.getInt(pos);
        int v1 = buffer.getInt(pos + Integer.BYTES);
        buffer.putInt(pos + Integer.BYTES, v1 - v0);
      }
      return;
    }
    int pos = buffer.position();

    // Encode in forward order, keeping the previous original value and delta in local variables
    // so we can safely overwrite each position in-place as we advance from index 2 onward.
    int prevPrevVal = buffer.getInt(pos);
    int prevVal = buffer.getInt(pos + Integer.BYTES);
    int prevDelta = prevVal - prevPrevVal;

    // Store first delta at index 1
    buffer.putInt(pos + Integer.BYTES, prevDelta);

    int prev = prevVal;
    int prevD = prevDelta;
    for (int i = 2; i < numValues; i++) {
      int offset = pos + i * Integer.BYTES;
      int curr = buffer.getInt(offset);
      int currDelta = curr - prev;
      int doubleDelta = currDelta - prevD;
      buffer.putInt(offset, doubleDelta);
      prev = curr;
      prevD = currDelta;
    }
  }

  private void encodeLongs(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Long.BYTES;
    if (numValues <= 2) {
      if (numValues == 2) {
        int pos = buffer.position();
        long v0 = buffer.getLong(pos);
        long v1 = buffer.getLong(pos + Long.BYTES);
        buffer.putLong(pos + Long.BYTES, v1 - v0);
      }
      return;
    }
    int pos = buffer.position();

    long prevPrevVal = buffer.getLong(pos);
    long prevVal = buffer.getLong(pos + Long.BYTES);
    long prevDelta = prevVal - prevPrevVal;

    buffer.putLong(pos + Long.BYTES, prevDelta);

    long prev = prevVal;
    long prevD = prevDelta;
    for (int i = 2; i < numValues; i++) {
      int offset = pos + i * Long.BYTES;
      long curr = buffer.getLong(offset);
      long currDelta = curr - prev;
      long doubleDelta = currDelta - prevD;
      buffer.putLong(offset, doubleDelta);
      prev = curr;
      prevD = currDelta;
    }
  }

  private void decodeInts(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Integer.BYTES;
    if (numValues <= 2) {
      if (numValues == 2) {
        int pos = buffer.position();
        int v0 = buffer.getInt(pos);
        int d1 = buffer.getInt(pos + Integer.BYTES);
        buffer.putInt(pos + Integer.BYTES, v0 + d1);
      }
      return;
    }
    int pos = buffer.position();

    int v0 = buffer.getInt(pos);
    int d1 = buffer.getInt(pos + Integer.BYTES);
    int v1 = v0 + d1;
    buffer.putInt(pos + Integer.BYTES, v1);

    int prev = v1;
    int prevDelta = d1;
    for (int i = 2; i < numValues; i++) {
      int offset = pos + i * Integer.BYTES;
      int doubleDelta = buffer.getInt(offset);
      int currDelta = prevDelta + doubleDelta;
      prev = prev + currDelta;
      buffer.putInt(offset, prev);
      prevDelta = currDelta;
    }
  }

  private void decodeLongs(ByteBuffer buffer, int numBytes) {
    int numValues = numBytes / Long.BYTES;
    if (numValues <= 2) {
      if (numValues == 2) {
        int pos = buffer.position();
        long v0 = buffer.getLong(pos);
        long d1 = buffer.getLong(pos + Long.BYTES);
        buffer.putLong(pos + Long.BYTES, v0 + d1);
      }
      return;
    }
    int pos = buffer.position();

    long v0 = buffer.getLong(pos);
    long d1 = buffer.getLong(pos + Long.BYTES);
    long v1 = v0 + d1;
    buffer.putLong(pos + Long.BYTES, v1);

    long prev = v1;
    long prevDelta = d1;
    for (int i = 2; i < numValues; i++) {
      int offset = pos + i * Long.BYTES;
      long doubleDelta = buffer.getLong(offset);
      long currDelta = prevDelta + doubleDelta;
      prev = prev + currDelta;
      buffer.putLong(offset, prev);
      prevDelta = currDelta;
    }
  }
}
