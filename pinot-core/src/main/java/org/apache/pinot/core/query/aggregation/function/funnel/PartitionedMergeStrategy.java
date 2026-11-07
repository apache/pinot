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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedValue;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;


class PartitionedMergeStrategy implements MergeStrategy<List<Long>> {
  /// Wire type value of step counts held in a `LongArrayList`
  private static final int LONG_ARRAY_LIST_TYPE = 43;
  /// Wire type value of a step count in the `List` wire type
  private static final int LONG_TYPE = 1;

  protected final int _numSteps;

  PartitionedMergeStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<Long> merge(List<Long> a, List<Long> b) {
    long[] result = toLongArray(a);
    for (int i = 0; i < _numSteps; i++) {
      result[i] += b.get(i);
    }
    return LongArrayList.wrap(result);
  }

  @Override
  public long[] extractFinalResult(List<Long> intermediateResult) {
    return toLongArray(intermediateResult);
  }

  /// Returns a copy of the step counts, which can be modified without modifying the given list.
  private static long[] toLongArray(List<Long> stepCounts) {
    if (stepCounts instanceof LongArrayList) {
      return ((LongArrayList) stepCounts).toLongArray();
    }
    int numSteps = stepCounts.size();
    long[] result = new long[numSteps];
    for (int i = 0; i < numSteps; i++) {
      result[i] = stepCounts.get(i);
    }
    return result;
  }

  @Override
  public boolean isMergeableAcrossGroups() {
    return false;
  }

  /// Serializes step counts held in a `LongArrayList`, which every segment result and merge produces, as the
  /// `LongArrayList` wire type, and any other list, such as the all-zero counts of a group without rows, as the `List`
  /// wire type.
  @Override
  public SerializedResult serializeIntermediateResult(List<Long> stepCounts) {
    if (stepCounts instanceof LongArrayList longArrayList) {
      return new SerializedValue<>(LONG_ARRAY_LIST_TYPE, longArrayList, ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE);
    }
    return new SerializedStepCounts(toLongArray(stepCounts));
  }

  @Override
  public List<Long> deserializeIntermediateResult(CustomObject customObject) {
    ByteBuffer byteBuffer = customObject.getBuffer();
    if (customObject.getType() == LONG_ARRAY_LIST_TYPE) {
      return ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.deserialize(byteBuffer);
    }
    int numSteps = byteBuffer.getInt();
    long[] stepCounts = new long[numSteps];
    if (numSteps > 0) {
      // Skip the state type, then read each count behind its length
      byteBuffer.getInt();
      for (int i = 0; i < numSteps; i++) {
        byteBuffer.getInt();
        stepCounts[i] = byteBuffer.getLong();
      }
    }
    return LongArrayList.wrap(stepCounts);
  }

  /// The serialized step counts as the `List` wire type: `[int size]`, then for a non-empty list `[int LONG_TYPE]` and
  /// `[int Long.BYTES][long count]` per step.
  private static final class SerializedStepCounts implements SerializedResult {
    private final long[] _stepCounts;

    SerializedStepCounts(long[] stepCounts) {
      _stepCounts = stepCounts;
    }

    @Override
    public int getType() {
      return LIST_TYPE;
    }

    @Override
    public int getSize() {
      int numSteps = _stepCounts.length;
      return numSteps == 0 ? Integer.BYTES : (2 + numSteps) * Integer.BYTES + numSteps * Long.BYTES;
    }

    @Override
    public byte[] getBytes() {
      byte[] bytes = new byte[getSize()];
      writeTo(ByteBuffer.wrap(bytes));
      return bytes;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
      int numSteps = _stepCounts.length;
      buffer.putInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      buffer.putInt(LONG_TYPE);
      for (long stepCount : _stepCounts) {
        buffer.putInt(Long.BYTES);
        buffer.putLong(stepCount);
      }
    }

    @Override
    public void writeTo(PinotOutputStream output)
        throws IOException {
      int numSteps = _stepCounts.length;
      output.writeInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      output.writeInt(LONG_TYPE);
      for (long stepCount : _stepCounts) {
        output.writeInt(Long.BYTES);
        output.writeLong(stepCount);
      }
    }
  }
}
