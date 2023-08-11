package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
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
    result[0] = stepsSets.get(0).size();
    for (int i = 1; i < _numSteps; i++) {
      // intersect this step with previous step
      stepsSets.get(i).retainAll(stepsSets.get(i - 1));
      result[i] = stepsSets.get(i).size();
    }
    return LongArrayList.wrap(result);
  }
}
