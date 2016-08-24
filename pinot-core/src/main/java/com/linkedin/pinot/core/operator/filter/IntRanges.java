/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.Pairs;


/**
 * Various utilities to deal with integer ranges, with both bounds being inclusive (eg. for docId ranges).
 */
public class IntRanges {
  private static final boolean ENABLE_PRECONDITION_CHECKS = false;

  /**
   * Clips a range in place, making sure that its lower value is greater or equals to lowerBound and its upper value is
   * smaller or equals to upperBound.
   *
   * @param range The range to clip
   * @param lowerBound The lower bound
   * @param upperBound The upper bound
   */
  public static void clip(Pairs.IntPair range, int lowerBound, int upperBound) {
    range.setLeft(Math.max(range.getLeft(), lowerBound));
    range.setRight(Math.min(range.getRight(), upperBound));
  }

  /**
   * Checks whether a range is invalid (upper bound lower to the lower bound)
   *
   * @return true if the range is invalid
   */
  public static boolean isInvalid(Pairs.IntPair range) {
    return range.getRight() < range.getLeft();
  }

  /**
   * Checks whether two ranges are mergeable (either overlapping or adjacent), assuming neither is degenerate.
   *
   * @param firstRange The first range to check
   * @param secondRange The second range to check
   */
  public static boolean rangesAreMergeable(Pairs.IntPair firstRange, Pairs.IntPair secondRange) {
    if (ENABLE_PRECONDITION_CHECKS) {
      Preconditions.checkArgument(!isInvalid(firstRange));
      Preconditions.checkArgument(!isInvalid(secondRange));
    }

    final boolean rangesAreAtLeastOneUnitApart =
        firstRange.getRight() < secondRange.getLeft() - 1 || secondRange.getRight() < firstRange.getLeft() - 1;
    return !rangesAreAtLeastOneUnitApart;
  }

  /**
   * Merge the second range into the first one, mutating the first one in place, assuming the ranges are not disjoint.
   *
   * @param firstRange The range to merge the second into
   * @param secondRange The range to merge with the first one
   */
  public static void mergeIntoFirst(Pairs.IntPair firstRange, Pairs.IntPair secondRange) {
    if (ENABLE_PRECONDITION_CHECKS) {
      Preconditions.checkArgument(rangesAreMergeable(firstRange, secondRange));
    }

    firstRange.setLeft(Math.min(firstRange.getLeft(), secondRange.getLeft()));
    firstRange.setRight(Math.max(firstRange.getRight(), secondRange.getRight()));
  }
}
