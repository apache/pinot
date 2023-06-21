package org.apache.pinot.core.operator.filter;

import java.util.OptionalInt;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;


/**
 * Operators that implements this interface can define how to be sorted in an AND, as defined in
 * {@link FilterOperatorUtils.Implementation#}.
 */
public interface PrioritizedFilterOperator<T extends Block> extends Operator<T> {

  int HIGH_PRIORITY = 0;
  int MEDIUM_PRIORITY = 1;
  int LOW_PRIORITY = 2;
  int AND_PRIORITY = 3;
  int OR_PRIORITY = 4;
  int SCAN_PRIORITY = 5;
  int EXPRESSION_PRIORITY = 10;

  /**
   * Priority is a number that is used to compare different filters. Some predicates, like AND, sort their sub
   * predicates in order to first apply the ones that should be more efficient.
   *
   * For example, {@link SortedIndexBasedFilterOperator} is assigned to {@link #HIGH_PRIORITY},
   * {@link BitmapBasedFilterOperator} is assigned to {@link #MEDIUM_PRIORITY} and {@link H3IndexFilterOperator} to
   * {@link #LOW_PRIORITY}
   *
   * @return the priority of this filter operator or an empty optional if no special priority should be used.
   */
  OptionalInt getPriority();
}
