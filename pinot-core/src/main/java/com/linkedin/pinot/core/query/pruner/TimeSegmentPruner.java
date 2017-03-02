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
package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;


/**
 * An implementation of SegmentPruner.
 * Pruner will prune segment if there is no overlapping of segment time interval and query
 * time interval.
 *
 *
 */
public class TimeSegmentPruner implements SegmentPruner {

  @Override
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    SegmentMetadata metadata = segment.getSegmentMetadata();
    String timeColumn = metadata.getTimeColumn();

    if (timeColumn == null) {
      return false;
    }

    TimeInterval segmentInterval = new TimeInterval(metadata.getStartTime(), metadata.getEndTime());
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    return (filterQueryTree != null && pruneSegment(filterQueryTree, segmentInterval, timeColumn));
  }

  /**
   * Helper method to determine if a segment can be pruned based on segment's time metadata and
   * the predicates on time column. It's algorithm is as follows:
   *
   * <ul>
   *   <li> For leaf node: Returns true if there is a predicate on time and apply the predicate would result
   *   in filtering out all docs of the segment, false otherwise. </li>
   *   <li> For non-leaf AND node: True if any of its children returned true, false otherwise. </li>
   *   <li> For non-leaf OR node: True if all its children returned true, false otherwise. </li>
   * </ul>
   *
   * @param filterQueryTree Filter tree for the query.
   * @param segmentInterval Time interval of the segment to be pruned.
   * @return True if segment can be pruned out, false otherwise.
   */
  public static boolean pruneSegment(@Nonnull FilterQueryTree filterQueryTree, @Nonnull TimeInterval segmentInterval,
      @Nonnull String timeColumn) {
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    // Leaf Node
    FilterOperator operator = filterQueryTree.getOperator();
    if (children == null || children.isEmpty()) {
      if (filterQueryTree.getColumn().equals(timeColumn)) {
        List<String> predicateValue = filterQueryTree.getValue();

        switch (operator) {
          case EQUALITY:
            return !segmentInterval.contains(Long.parseLong(predicateValue.get(0)));

          case RANGE:
            return !segmentInterval.overlaps(getIntervalFromString(timeColumn, predicateValue));

          default:
            return false; // Only prune for equality and range, for now.
        }
      } else {
        return false;
      }
    } else {

      switch (operator) {
        case AND:
          for (FilterQueryTree child : children) {
            if (pruneSegment(child, segmentInterval, timeColumn)) {
              return true;
            }
          }
          return false;

        case OR:
          for (FilterQueryTree child : children) {
            if (!pruneSegment(child, segmentInterval, timeColumn)) {
              return false;
            }
          }
          return true;

        default:
          throw new IllegalArgumentException(
              "Illegal operator type in filter query tree: " + operator.getClass().getName());
      }
    }
  }

  @Override
  public void init(Configuration config) {

  }

  @Override
  public String toString() {
    return "TimeSegmentPruner";
  }

  /**
   * Helper method that generates TimeInterval from a RangePredicate string.
   *
   * @param rangeString Range String
   * @return TimeInterval for the rangeString
   */
  private static TimeInterval getIntervalFromString(String timeColumn, List<String> rangeString) {
    RangePredicate range = new RangePredicate(timeColumn, rangeString);
    String lowerString = range.getLowerBoundary();

    // Given that segment's time range is guaranteed to be between
    // TimeUtils.VALID_MIN_TIME_MILLIS and TimeUtils.VALID_MAX_TIME_MILLIS (very small portion of long range),
    // it is OK to be avoid long overflow when converting exclusive to inclusive.
    long lower = lowerString.equals(RangePredicate.UNBOUNDED) ? 0 : Long.parseLong(lowerString);
    lower = (range.includeLowerBoundary() || lower == Long.MAX_VALUE) ? lower : lower + 1;

    String upperString = range.getUpperBoundary();
    long upper = upperString.equals(RangePredicate.UNBOUNDED) ? Long.MAX_VALUE : Long.parseLong(upperString);
    upper = (range.includeUpperBoundary() || upper == Long.MIN_VALUE) ? upper : upper - 1;

    return new TimeInterval(lower, upper);
  }

  /**
   * Utility class to represent time interval, with start and end both inclusive.
   */
  public static class TimeInterval {
    private final long _start;
    private final long _end;

    public TimeInterval(long start, long end) {
      _start = start;
      _end = end;
    }

    public long getStart() {
      return _start;
    }

    public long getEnd() {
      return _end;
    }

    public boolean contains(long value) {
      return (value >= _start && value <= _end);
    }

    public boolean overlaps(TimeInterval that) {
      // Degenerate case: any one of the intervals is inverted, so it cannot overlap with the other.
      if (this._start > this._end || that._start > that._end) {
        return false;
      }
      return !((this._start > that._end) || (this._end < that._start));
    }
  }
}
