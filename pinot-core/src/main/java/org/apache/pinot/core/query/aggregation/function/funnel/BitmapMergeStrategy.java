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
import java.util.List;
import org.roaringbitmap.RoaringBitmap;


class BitmapMergeStrategy implements MergeStrategy<List<RoaringBitmap>> {
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
  public LongArrayList extractFinalResult(List<RoaringBitmap> stepsBitmaps) {
    long[] result = new long[_numSteps];
    result[0] = stepsBitmaps.get(0).getCardinality();
    for (int i = 1; i < _numSteps; i++) {
      // intersect this step with previous step
      stepsBitmaps.get(i).and(stepsBitmaps.get(i - 1));
      result[i] = stepsBitmaps.get(i).getCardinality();
    }
    return LongArrayList.wrap(result);
  }
}
