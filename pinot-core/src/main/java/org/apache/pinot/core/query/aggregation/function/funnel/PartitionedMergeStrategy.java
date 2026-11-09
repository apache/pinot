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


class PartitionedMergeStrategy implements MergeStrategy<List<Long>> {
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
}
