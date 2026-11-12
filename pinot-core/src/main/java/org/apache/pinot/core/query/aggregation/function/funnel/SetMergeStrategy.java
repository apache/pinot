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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils.SizedObjectSerDe;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;


@SuppressWarnings({"rawtypes", "unchecked"})
class SetMergeStrategy implements MergeStrategy<List<Set>> {
  // Wire type values of the step sets
  private static final int INT_SET_TYPE = 9;
  private static final int LONG_SET_TYPE = 15;
  private static final int FLOAT_SET_TYPE = 16;
  private static final int DOUBLE_SET_TYPE = 17;
  private static final int STRING_SET_TYPE = 18;

  protected final int _numSteps;

  SetMergeStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<Set> merge(List<Set> intermediateResult1, List<Set> intermediateResult2) {
    // A segment without rows contributes empty string sets whatever the stored type of the correlation column. Take the
    // other side as a whole when one side has no values, so that the step sets of one list keep a single set type.
    if (hasNoValues(intermediateResult2)) {
      return intermediateResult1;
    }
    if (hasNoValues(intermediateResult1)) {
      return intermediateResult2;
    }
    for (int i = 0; i < _numSteps; i++) {
      intermediateResult1.get(i).addAll(intermediateResult2.get(i));
    }
    return intermediateResult1;
  }

  /// Returns whether all step sets are empty, as they are for a segment without rows.
  private static boolean hasNoValues(List<Set> stepsSets) {
    for (Set stepSet : stepsSets) {
      if (!stepSet.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long[] extractFinalResult(List<Set> stepsSets) {
    long[] result = new long[_numSteps];
    Set intersection = stepsSets.get(0);
    result[0] = intersection.size();
    for (int i = 1; i < _numSteps; i++) {
      Set step = stepsSets.get(i);
      if (intersection.isEmpty() || step.isEmpty()) {
        break;
      }
      // Preserve the intermediate sets and copy only the smaller population.
      if (i == 1 || intersection.size() > step.size()) {
        intersection = intersection.size() <= step.size()
            ? intersect(intersection, step)
            : intersect(step, intersection);
      } else {
        intersection.retainAll(step);
      }
      result[i] = intersection.size();
    }
    return result;
  }

  private static Set intersect(Set smaller, Set larger) {
    if (smaller instanceof IntSet intSet && larger instanceof IntSet otherIntSet) {
      IntSet intersection = new IntOpenHashSet(intSet);
      intersection.retainAll(otherIntSet);
      return intersection;
    }
    if (smaller instanceof LongSet longSet && larger instanceof LongSet otherLongSet) {
      LongSet intersection = new LongOpenHashSet(longSet);
      intersection.retainAll(otherLongSet);
      return intersection;
    }
    if (smaller instanceof FloatSet floatSet && larger instanceof FloatSet otherFloatSet) {
      FloatSet intersection = new FloatOpenHashSet(floatSet);
      intersection.retainAll(otherFloatSet);
      return intersection;
    }
    if (smaller instanceof DoubleSet doubleSet && larger instanceof DoubleSet otherDoubleSet) {
      DoubleSet intersection = new DoubleOpenHashSet(doubleSet);
      intersection.retainAll(otherDoubleSet);
      return intersection;
    }
    Set intersection = new HashSet<>(smaller);
    intersection.retainAll(larger);
    return intersection;
  }

  @Override
  public SerializedResult serializeIntermediateResult(List<Set> stepsSets) {
    return new SerializedStepsSets(stepsSets);
  }

  @Override
  public List<Set> deserializeIntermediateResult(CustomObject customObject) {
    ByteBuffer byteBuffer = customObject.getBuffer();
    int numSteps = byteBuffer.getInt();
    List<Set> stepsSets = new ArrayList<>(numSteps);
    if (numSteps == 0) {
      return stepsSets;
    }
    SizedObjectSerDe<Set> setSerDe = setSerDe(byteBuffer.getInt());
    for (int i = 0; i < numSteps; i++) {
      int numBytes = byteBuffer.getInt();
      stepsSets.add(setSerDe.deserialize(byteBuffer.slice().limit(numBytes)));
      byteBuffer.position(byteBuffer.position() + numBytes);
    }
    return stepsSets;
  }

  /// Returns the wire type value of a step set. The sets of one list share the stored type of the correlation column,
  /// so they are int, long, float, double or string sets, and an empty step set is a string set.
  private static int setType(Set set) {
    return switch (set) {
      case IntSet _ -> INT_SET_TYPE;
      case LongSet _ -> LONG_SET_TYPE;
      case FloatSet _ -> FLOAT_SET_TYPE;
      case DoubleSet _ -> DOUBLE_SET_TYPE;
      default -> STRING_SET_TYPE;
    };
  }

  /// Returns the codec of the step sets of a wire type value.
  private static SizedObjectSerDe<Set> setSerDe(int setType) {
    SizedObjectSerDe<? extends Set> setSerDe = switch (setType) {
      case INT_SET_TYPE -> ObjectSerDeUtils.INT_SET_SER_DE;
      case LONG_SET_TYPE -> ObjectSerDeUtils.LONG_SET_SER_DE;
      case FLOAT_SET_TYPE -> ObjectSerDeUtils.FLOAT_SET_SER_DE;
      case DOUBLE_SET_TYPE -> ObjectSerDeUtils.DOUBLE_SET_SER_DE;
      case STRING_SET_TYPE -> ObjectSerDeUtils.STRING_SET_SER_DE;
      default -> throw new IllegalStateException("Unexpected funnel step set type: " + setType);
    };
    return (SizedObjectSerDe<Set>) setSerDe;
  }

  /// The serialized step sets as the `List` wire type: `[int size]`, then for a non-empty list `[int set type]` and
  /// `[int length][set bytes]` per step. The sets are sized up front and written on demand, so they must not change
  /// until written.
  private static final class SerializedStepsSets implements SerializedResult {
    private final List<Set> _stepsSets;
    private final int _setType;
    private final SizedObjectSerDe<Set> _setSerDe;
    private final int[] _setSizes;
    private final int _size;

    SerializedStepsSets(List<Set> stepsSets) {
      _stepsSets = stepsSets;
      int numSteps = stepsSets.size();
      _setType = numSteps > 0 ? setType(stepsSets.get(0)) : STRING_SET_TYPE;
      _setSerDe = setSerDe(_setType);
      _setSizes = new int[numSteps];
      int size = numSteps == 0 ? Integer.BYTES : (2 + numSteps) * Integer.BYTES;
      for (int i = 0; i < numSteps; i++) {
        int setSize = _setSerDe.getSerializedSize(stepsSets.get(i));
        _setSizes[i] = setSize;
        size = Math.addExact(size, setSize);
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
      int numSteps = _setSizes.length;
      buffer.putInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      buffer.putInt(_setType);
      for (int i = 0; i < numSteps; i++) {
        buffer.putInt(_setSizes[i]);
        _setSerDe.serialize(_stepsSets.get(i), buffer);
      }
    }

    @Override
    public void writeTo(PinotOutputStream output)
        throws IOException {
      int numSteps = _setSizes.length;
      output.writeInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      output.writeInt(_setType);
      for (int i = 0; i < numSteps; i++) {
        output.writeInt(_setSizes[i]);
        _setSerDe.serialize(_stepsSets.get(i), output);
      }
    }
  }
}
