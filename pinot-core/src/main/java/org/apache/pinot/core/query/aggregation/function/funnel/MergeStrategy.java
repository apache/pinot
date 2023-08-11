package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import javax.annotation.concurrent.ThreadSafe;


/**
 * Interface for cross-segment merge strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
 *
 * @param <I> Intermediate result at segment level (extracted from aggregation strategy result).
 */
@ThreadSafe
interface MergeStrategy<I> {
  I merge(I intermediateResult1, I intermediateResult2);

  LongArrayList extractFinalResult(I intermediateResult);
}
