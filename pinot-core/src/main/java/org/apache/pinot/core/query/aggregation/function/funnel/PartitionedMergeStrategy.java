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
