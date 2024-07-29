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
    LongArrayList result = toLongArrayList(a);
    long[] elements = result.elements();
    for (int i = 0; i < _numSteps; i++) {
      elements[i] += b.get(i);
    }
    return result;
  }

  @Override
  public LongArrayList extractFinalResult(List<Long> intermediateResult) {
    return toLongArrayList(intermediateResult);
  }

  private LongArrayList toLongArrayList(List<Long> longList) {
    return longList instanceof LongArrayList ? ((LongArrayList) longList).clone() : new LongArrayList(longList);
  }
}
