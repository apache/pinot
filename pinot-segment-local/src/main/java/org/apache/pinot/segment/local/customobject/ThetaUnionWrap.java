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
package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;


/**
 * Intermediate state used by {@code DistinctCountThetaSketchAggregationFunction} which re-uses the same union operation
 * as much as possible for performance reasons.
 * The union operation initialises an empty "gadget" sketch that is updated with hashed entries that fall below the
 * minimum Theta value for all input sketches ("Broder Rule").  If the input sketches are ordered, the "early-stop"
 * optimisation allows the input to be merged quickly without having to scan all the retained items in the input sketch.
 */
public class ThetaUnionWrap {
  private Union _union;
  // Log-base 2 of the Resize Factor.
  private int _lgRf = 3;
  // Log-base 2 Nominal Entries
  private int _lgNoms = 4;
  // Pre-sampling probability p
  private float _p = 1.0f;
  private boolean _ordered = false;
  private boolean _isEmpty = true;

  public ThetaUnionWrap(SetOperationBuilder setOperationBuilder, boolean ordered) {
    _p = setOperationBuilder.getP();
    _lgNoms = setOperationBuilder.getLgNominalEntries();
    _lgRf = setOperationBuilder.getResizeFactor().lg();
    _ordered = ordered;
    _union = setOperationBuilder.buildUnion();
  }

  public boolean isEmpty() {
    return _isEmpty;
  }

  @Nonnull
  public Sketch getResult() {
    return _union.getResult(_ordered, null);
  }

  public void apply(Sketch sketch) {
    internalAdd(sketch);
  }

  public void merge(ThetaUnionWrap thetaUnion) {
    if (thetaUnion.isEmpty()) {
      return;
    }
    Sketch sketch = thetaUnion.getResult();
    internalAdd(sketch);
  }

  @Nonnull
  public byte[] toBytes() {
    Sketch sketch = _union.getResult(_ordered, null);
    byte[] sketchBytes = sketch.toByteArray();
    int capacity = Float.BYTES + 3 + sketchBytes.length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);
    byteBuffer.putFloat(_p);
    byteBuffer.put((byte) _lgRf);
    byteBuffer.put((byte) _lgNoms);
    byteBuffer.put((byte) (_ordered ? 1 : 0));
    byteBuffer.put(sketchBytes);
    return byteBuffer.array();
  }

  @Nonnull
  public static ThetaUnionWrap fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @Nonnull
  public static ThetaUnionWrap fromByteBuffer(ByteBuffer byteBuffer) {
    Float p = byteBuffer.getFloat();
    int lgRf = byteBuffer.get() & 0xFF;
    int lgNoms = byteBuffer.get() & 0xFF;
    boolean ordered = byteBuffer.get() != 0;
    byte[] sketchBytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(sketchBytes);

    SetOperationBuilder builder = new SetOperationBuilder();
    builder.setP(p);
    builder.setResizeFactor(ResizeFactor.getRF(lgRf));
    builder.setLogNominalEntries(lgNoms);
    ThetaUnionWrap unionWrap = new ThetaUnionWrap(builder, ordered);
    Sketch sketch = Sketches.wrapSketch(Memory.wrap(sketchBytes));
    unionWrap.apply(sketch);

    return unionWrap;
  }

  private void internalAdd(Sketch sketch) {
    if (sketch.isEmpty()) {
      return;
    }
    _isEmpty = false;
    _union.union(sketch);
  }
}
