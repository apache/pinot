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
package org.apache.pinot.core.query.aggregation.function.funnel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;
import org.roaringbitmap.RoaringBitmap;


class BitmapMergeStrategy implements MergeStrategy<List<RoaringBitmap>> {
  /// Wire type value of a step bitmap
  private static final int ROARING_BITMAP_TYPE = 14;

  protected final int _numSteps;

  BitmapMergeStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<RoaringBitmap> merge(List<RoaringBitmap> intermediateResult1, List<RoaringBitmap> intermediateResult2) {
    for (int i = 0; i < _numSteps; i++) {
      intermediateResult1.get(i).or(intermediateResult2.get(i));
    }
    return intermediateResult1;
  }

  @Override
  public long[] extractFinalResult(List<RoaringBitmap> stepsBitmaps) {
    long[] result = new long[_numSteps];
    RoaringBitmap intersection = stepsBitmaps.get(0);
    result[0] = intersection.getCardinality();
    for (int i = 1; i < _numSteps; i++) {
      RoaringBitmap step = stepsBitmaps.get(i);
      if (intersection.isEmpty() || step.isEmpty()) {
        break;
      }
      if (i == 1) {
        intersection = RoaringBitmap.and(intersection, step);
      } else {
        intersection.and(step);
      }
      result[i] = intersection.getCardinality();
    }
    return result;
  }

  @Override
  public SerializedResult serializeIntermediateResult(List<RoaringBitmap> stepsBitmaps) {
    return new SerializedStepsBitmaps(stepsBitmaps);
  }

  @Override
  public List<RoaringBitmap> deserializeIntermediateResult(CustomObject customObject) {
    ByteBuffer byteBuffer = customObject.getBuffer();
    int numSteps = byteBuffer.getInt();
    List<RoaringBitmap> stepsBitmaps = new ArrayList<>(numSteps);
    if (numSteps == 0) {
      return stepsBitmaps;
    }
    // Skip the state type, then read each bitmap behind its length
    byteBuffer.getInt();
    for (int i = 0; i < numSteps; i++) {
      int numBytes = byteBuffer.getInt();
      stepsBitmaps.add(RoaringBitmapUtils.deserialize(byteBuffer.slice().limit(numBytes)));
      byteBuffer.position(byteBuffer.position() + numBytes);
    }
    return stepsBitmaps;
  }

  /// The serialized step bitmaps as the `List` wire type: `[int size]`, then for a non-empty list
  /// `[int ROARING_BITMAP_TYPE]` and `[int length][bitmap bytes]` per step. The bitmaps are sized up front and written
  /// on demand, so they must not change until written.
  private static final class SerializedStepsBitmaps implements SerializedResult {
    private final List<RoaringBitmap> _stepsBitmaps;
    private final int[] _bitmapSizes;
    private final int _size;

    SerializedStepsBitmaps(List<RoaringBitmap> stepsBitmaps) {
      _stepsBitmaps = stepsBitmaps;
      int numSteps = stepsBitmaps.size();
      _bitmapSizes = new int[numSteps];
      int size = numSteps == 0 ? Integer.BYTES : (2 + numSteps) * Integer.BYTES;
      for (int i = 0; i < numSteps; i++) {
        int bitmapSize = stepsBitmaps.get(i).serializedSizeInBytes();
        _bitmapSizes[i] = bitmapSize;
        size = Math.addExact(size, bitmapSize);
      }
      _size = size;
    }

    @Override
    public int getType() {
      return LIST_TYPE;
    }

    @Override
    public int getSize() {
      return _size;
    }

    @Override
    public byte[] getBytes() {
      byte[] bytes = new byte[_size];
      writeTo(ByteBuffer.wrap(bytes));
      return bytes;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
      int numSteps = _bitmapSizes.length;
      buffer.putInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      buffer.putInt(ROARING_BITMAP_TYPE);
      for (int i = 0; i < numSteps; i++) {
        buffer.putInt(_bitmapSizes[i]);
        RoaringBitmapUtils.serialize(_stepsBitmaps.get(i), buffer);
      }
    }

    @Override
    public void writeTo(PinotOutputStream output)
        throws IOException {
      int numSteps = _bitmapSizes.length;
      output.writeInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      output.writeInt(ROARING_BITMAP_TYPE);
      for (int i = 0; i < numSteps; i++) {
        output.writeInt(_bitmapSizes[i]);
        output.write(RoaringBitmapUtils.serialize(_stepsBitmaps.get(i)));
      }
    }
  }
}
