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

package com.linkedin.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.Comparator;


/**
 * A document id range, which is inclusive on both ends. For example, a range of [1,3] would include documents 1, 2 and
 * 3, but not 0 or 4.
 */
public class DocIdRange {
  private static final boolean ENABLE_PRECONDITION_CHECKS = false;
  int start;
  int end;

  public DocIdRange(int start, int end) {
    this.start = start;
    this.end = end;
  }

  /**
   * Clips a range, making sure that its start value is greater or equals to the start of the clipping range and that
   * its end value is smaller or equals to the end of the clipping range.
   *
   * @param clipRange The range to clip this range with
   */
  public void clip(DocIdRange clipRange) {
    start = Math.max(start, clipRange.start);
    end = Math.min(end, clipRange.end);
  }

  /**
   * Checks whether this range is invalid (upper bound lower to the lower bound)
   *
   * @return true if the range is invalid
   */
  public boolean isInvalid() {
    return end < start;
  }

  /**
   * Checks whether ranges are mergeable (either overlapping or adjacent), assuming neither is invalid.
   *
   * @param otherRange The range to check for mergeability
   * @return true if the given range can be merged
   */
  public boolean rangeIsMergeable(DocIdRange otherRange) {
    if (ENABLE_PRECONDITION_CHECKS) {
      Preconditions.checkArgument(!isInvalid());
      Preconditions.checkArgument(!otherRange.isInvalid());
    }

    final boolean rangesAreAtLeastOneUnitApart = end < otherRange.start - 1 || otherRange.end < start - 1;
    return !rangesAreAtLeastOneUnitApart;
  }

  /**
   * Merge the given range into this one, assuming the ranges are not disjoint.
   *
   * @param otherRange The range to merge with
   */
  public void mergeWithRange(DocIdRange otherRange) {
    if (ENABLE_PRECONDITION_CHECKS) {
      Preconditions.checkArgument(rangeIsMergeable(otherRange));
    }

    start = Math.min(start, otherRange.start);
    end = Math.max(end, otherRange.end);
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  @Override
  public String toString() {
    return "[" + start + "," + end + "]";
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(start, end);
  }

  @Override
  public boolean equals(Object obj) {
    if (EqualityUtils.isNullOrNotSameClass(this, obj)) {
      return false;
    }

    if (EqualityUtils.isSameReference(this, obj)) {
      return true;
    }

    DocIdRange other = (DocIdRange) obj;
    return EqualityUtils.isEqual(this.start, other.start) && EqualityUtils.isEqual(this.end, other.end);
  }

  /**
   * Returns a DocIdRange comparator that sorts the given ranges in ascending order.
   * @return A DocIdRange comparator that sorts the given ranges in ascending order.
   */
  public static Comparator<DocIdRange> buildAscendingDocIdRangeComparator() {
    return new AscendingDocIdRangeComparator();
  }

  private static class AscendingDocIdRangeComparator implements Comparator<DocIdRange> {
    @Override
    public int compare(DocIdRange o1, DocIdRange o2) {
      return Integer.compare(o1.start, o2.start);
    }
  }
}
