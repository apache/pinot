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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


class SetMergeStrategy implements MergeStrategy<List<Set>> {
  protected final int _numSteps;

  SetMergeStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<Set> merge(List<Set> intermediateResult1, List<Set> intermediateResult2) {
    for (int i = 0; i < _numSteps; i++) {
      intermediateResult1.get(i).addAll(intermediateResult2.get(i));
    }
    return intermediateResult1;
  }

  @Override
  public LongArrayList extractFinalResult(List<Set> stepsSets) {
    long[] result = new long[_numSteps];
    Set<?> intersection = stepsSets.get(0);
    result[0] = intersection.size();
    for (int i = 1; i < _numSteps; i++) {
      Set<?> step = stepsSets.get(i);
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
    return LongArrayList.wrap(result);
  }

  private static Set<?> intersect(Set<?> smaller, Set<?> larger) {
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
    Set<?> intersection = new HashSet<>(smaller);
    intersection.retainAll(larger);
    return intersection;
  }
}
