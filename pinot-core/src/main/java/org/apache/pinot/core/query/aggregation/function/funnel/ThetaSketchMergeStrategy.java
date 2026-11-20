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
import org.apache.datasketches.theta.ThetaIntersection;
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;


class ThetaSketchMergeStrategy implements MergeStrategy<List<ThetaSketch>> {
  /// Wire type value of a step sketch
  private static final int DATA_SKETCH_TYPE = 12;

  protected final int _numSteps;
  final ThetaSetOperationBuilder _setOperationBuilder;

  ThetaSketchMergeStrategy(int numSteps, int nominalEntries) {
    _numSteps = numSteps;
    _setOperationBuilder = new ThetaSetOperationBuilder().setNominalEntries(nominalEntries);
  }

  @Override
  public List<ThetaSketch> merge(List<ThetaSketch> sketches1, List<ThetaSketch> sketches2) {
    final List<ThetaSketch> mergedSketches = new ArrayList<>(_numSteps);
    for (int i = 0; i < _numSteps; i++) {
      // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
      //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
      mergedSketches.add(_setOperationBuilder.buildUnion().union(sketches1.get(i), sketches2.get(i), false, null));
    }
    return mergedSketches;
  }

  @Override
  public long[] extractFinalResult(List<ThetaSketch> sketches) {
    long[] result = new long[_numSteps];

    ThetaSketch sketch = sketches.get(0);
    result[0] = Math.round(sketch.getEstimate());
    for (int i = 1; i < _numSteps; i++) {
      ThetaIntersection intersection = _setOperationBuilder.buildIntersection();
      sketch = intersection.intersect(sketch, sketches.get(i));
      result[i] = Math.round(sketch.getEstimate());
    }
    return result;
  }

  @Override
  public SerializedResult serializeIntermediateResult(List<ThetaSketch> stepsSketches) {
    return new SerializedStepsSketches(stepsSketches);
  }

  @Override
  public List<ThetaSketch> deserializeIntermediateResult(CustomObject customObject) {
    ByteBuffer byteBuffer = customObject.getBuffer();
    int numSteps = byteBuffer.getInt();
    List<ThetaSketch> stepsSketches = new ArrayList<>(numSteps);
    if (numSteps == 0) {
      return stepsSketches;
    }
    // Skip the state type, then read each sketch behind its length
    byteBuffer.getInt();
    for (int i = 0; i < numSteps; i++) {
      int numBytes = byteBuffer.getInt();
      stepsSketches.add(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(byteBuffer.slice().limit(numBytes)));
      byteBuffer.position(byteBuffer.position() + numBytes);
    }
    return stepsSketches;
  }

  /// The serialized step sketches as the `List` wire type: `[int size]`, then for a non-empty list
  /// `[int DATA_SKETCH_TYPE]` and `[int length][sketch bytes]` per step. The sketches are sized up front and written
  /// on demand, so they must not change until written.
  private static final class SerializedStepsSketches implements SerializedResult {
    private final List<ThetaSketch> _stepsSketches;
    private final int[] _sketchSizes;
    private final int _size;

    SerializedStepsSketches(List<ThetaSketch> stepsSketches) {
      _stepsSketches = stepsSketches;
      int numSteps = stepsSketches.size();
      _sketchSizes = new int[numSteps];
      int size = numSteps == 0 ? Integer.BYTES : (2 + numSteps) * Integer.BYTES;
      for (int i = 0; i < numSteps; i++) {
        int sketchSize = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.getSerializedSize(stepsSketches.get(i));
        _sketchSizes[i] = sketchSize;
        size = Math.addExact(size, sketchSize);
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
      int numSteps = _sketchSizes.length;
      buffer.putInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      buffer.putInt(DATA_SKETCH_TYPE);
      for (int i = 0; i < numSteps; i++) {
        buffer.putInt(_sketchSizes[i]);
        ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(_stepsSketches.get(i), buffer);
      }
    }

    @Override
    public void writeTo(PinotOutputStream output)
        throws IOException {
      int numSteps = _sketchSizes.length;
      output.writeInt(numSteps);
      if (numSteps == 0) {
        return;
      }
      output.writeInt(DATA_SKETCH_TYPE);
      for (int i = 0; i < numSteps; i++) {
        output.writeInt(_sketchSizes[i]);
        output.write(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(_stepsSketches.get(i)));
      }
    }
  }
}
