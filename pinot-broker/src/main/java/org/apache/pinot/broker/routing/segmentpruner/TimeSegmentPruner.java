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
package org.apache.pinot.broker.routing.segmentpruner;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.segmentpruner.interval.Interval;
import org.apache.pinot.broker.routing.segmentpruner.interval.IntervalTree;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimeSegmentPruner} prunes segments based on their time column start & end time metadata stored in ZK. The pruner
 * supports queries with filter (or nested filter) of EQUALITY and RANGE predicates.
 */
public class TimeSegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSegmentPruner.class);
  private static final long MIN_START_TIME = 0;
  private static final long MAX_END_TIME = Long.MAX_VALUE;
  private static final Interval DEFAULT_INTERVAL = new Interval(MIN_START_TIME, MAX_END_TIME);

  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final String _timeColumn;
  private final DateTimeFormatSpec _timeFormatSpec;

  private volatile IntervalTree<String> _intervalTree;
  private final Map<String, Interval> _intervalMap = new HashMap<>();

  public TimeSegmentPruner(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
    _timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    Preconditions
        .checkNotNull(_timeColumn, "Time column must be configured in table config for table: %s", _tableNameWithType);

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkNotNull(schema, "Failed to find schema for table: %s", _tableNameWithType);
    DateTimeFieldSpec dateTimeSpec = schema.getSpecForTimeColumn(_timeColumn);
    Preconditions.checkNotNull(dateTimeSpec, "Field spec must be specified in schema for time column: %s of table: %s",
        _timeColumn, _tableNameWithType);
    _timeFormatSpec = new DateTimeFormatSpec(dateTimeSpec.getFormat());
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    // Bulk load time info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      Interval interval = extractIntervalFromSegmentZKMetaZNRecord(segment, znRecords.get(i));
      _intervalMap.put(segment, interval);
    }
    _intervalTree = new IntervalTree<>(_intervalMap);
  }

  private Interval extractIntervalFromSegmentZKMetaZNRecord(String segment, @Nullable ZNRecord znRecord) {
    // Segments without metadata or with invalid time interval will be set with [min_start, max_end] and will not be pruned
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return DEFAULT_INTERVAL;
    }

    long startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    long endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (startTime < 0 || endTime < 0 || startTime > endTime) {
      LOGGER.warn("Failed to find valid time interval for segment: {}, table: {}", segment, _tableNameWithType);
      return DEFAULT_INTERVAL;
    }

    TimeUnit timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    return new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime));
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (String segment : onlineSegments) {
      _intervalMap.computeIfAbsent(segment, k -> extractIntervalFromSegmentZKMetaZNRecord(k,
          _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT)));
    }
    _intervalMap.keySet().retainAll(onlineSegments);
    _intervalTree = new IntervalTree<>(_intervalMap);
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    Interval interval = extractIntervalFromSegmentZKMetaZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    _intervalMap.put(segment, interval);
    _intervalTree = new IntervalTree<>(_intervalMap);
  }

  /**
   * NOTE: Pruning is done by searching _intervalTree based on request time interval and check if the results
   *       are in the input segments. By doing so we will have run time O(M * logN) (N: # of all online segments,
   *       M: # of qualified intersected segments).
   */
  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    IntervalTree<String> intervalTree = _intervalTree;
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (filterQueryTree == null) {
      return segments;
    }
    List<Interval> intervals = getFilterTimeIntervals(filterQueryTree);
    if (intervals == null) { // cannot prune based on time for input request
      return segments;
    }
    if (intervals.isEmpty()) { // invalid query time interval
      return Collections.emptySet();
    }

    Set<String> selectedSegments = new HashSet<>();
    for (Interval interval : intervals) {
      for (String segment : intervalTree.searchAll(interval)) {
        if (segments.contains(segment)) {
          selectedSegments.add(segment);
        }
      }
    }
    return selectedSegments;
  }

  /**
   * @return Null if no time condition or cannot filter base on the condition (e.g. 'SELECT * from myTable where time < 50 OR firstName = Jason')
   * @return Empty list if time condition is specified but invalid (e.g. 'SELECT * from myTable where time < 50 AND time > 100')
   */
  @Nullable
  private List<Interval> getFilterTimeIntervals(FilterQueryTree filterQueryTree) {
    switch (filterQueryTree.getOperator()) {
      case AND:
        List<List<Interval>> andIntervals = new ArrayList<>();
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          List<Interval> childIntervals = getFilterTimeIntervals(child);
          if (childIntervals != null) {
            if (childIntervals.isEmpty()) {
              return Collections.emptyList();
            }
            andIntervals.add(childIntervals);
          }
        }
        if (andIntervals.isEmpty()) {
          return null;
        }
        return getIntersectionSortedIntervals(andIntervals);
      case OR:
        List<List<Interval>> orIntervals = new ArrayList<>();
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          List<Interval> childIntervals = getFilterTimeIntervals(child);
          if (childIntervals == null) {
            return null;
          } else {
            orIntervals.add(childIntervals);
          }
        }
        return getUnionSortedIntervals(orIntervals);
      case EQUALITY:
        if (filterQueryTree.getColumn().equals(_timeColumn)) {
          long timeStamp = _timeFormatSpec.fromFormatToMillis(filterQueryTree.getValue().get(0));
          return Collections.singletonList(new Interval(timeStamp, timeStamp));
        }
        return null;
      case IN:
        if (filterQueryTree.getColumn().equals(_timeColumn)) {
          List<Interval> intervals = new ArrayList<>(filterQueryTree.getValue().size());
          for (String value : filterQueryTree.getValue()) {
            long timeStamp = _timeFormatSpec.fromFormatToMillis(value);
            intervals.add(new Interval(timeStamp, timeStamp));
          }
          return intervals;
        }
        return null;
      case RANGE:
        if (filterQueryTree.getColumn().equals(_timeColumn)) {
          return parseInterval(filterQueryTree.getValue());
        }
        return null;
      default:
        return null;
    }
  }

  private List<Interval> getIntersectionSortedIntervals(List<List<Interval>> intervals) {
    // Requires input intervals are sorted, the return intervals will be sorted
    return getIntersectionSortedIntervals(intervals, 0, intervals.size());
  }

  private List<Interval> getIntersectionSortedIntervals(List<List<Interval>> intervals, int start, int end) {
    if (start + 1 == end) {
      return intervals.get(start);
    }

    int mid = start + (end - start) / 2;
    List<Interval> interval1 = getIntersectionSortedIntervals(intervals, start, mid);
    List<Interval> interval2 = getIntersectionSortedIntervals(intervals, mid, end);
    return getIntersectionTwoSortedIntervals(interval1, interval2);
  }

  /**
   * Intersect two list of non-overlapping sorted intervals.
   * E.g. {[1, 3], [4, 6], [7, 8], [10, 10]} and {[2, 5], [7, 9]} are merged as {[2, 3], [4, 5], [7, 8]}
   */
  private List<Interval> getIntersectionTwoSortedIntervals(List<Interval> intervals1, List<Interval> intervals2) {
    List<Interval> res = new ArrayList<>();
    int size1 = intervals1.size();
    int size2 = intervals2.size();
    int i = 0;
    int j = 0;
    while (i < size1 && j < size2) {
      Interval interval1 = intervals1.get(i);
      Interval interval2 = intervals2.get(j);
      if (interval1.intersects(interval2)) {
        res.add(Interval.getIntersection(interval1, interval2));
      }
      if (interval1._max <= interval2._max) {
        i++;
      } else {
        j++;
      }
    }
    return res;
  }

  private List<Interval> getUnionSortedIntervals(List<List<Interval>> intervals) {
    // Requires input intervals are sorted, the return intervals will be sorted
    return getUnionSortedIntervals(intervals, 0, intervals.size());
  }

  private List<Interval> getUnionSortedIntervals(List<List<Interval>> intervals, int start, int end) {
    if (start + 1 == end) {
      return intervals.get(start);
    }

    int mid = start + (end - start) / 2;
    List<Interval> intervals1 = getUnionSortedIntervals(intervals, start, mid);
    List<Interval> intervals2 = getUnionSortedIntervals(intervals, mid, end);
    return getUnionTwoSortedIntervals(intervals1, intervals2);
  }

  /**
   * Union two list of non-overlapping sorted intervals.
   * E.g. {[1, 2], [5, 7], [9, 10]} and {[2, 3], [4, 8]} are merged as {[1, 3], [4, 8], [9, 10]}
   */
  private List<Interval> getUnionTwoSortedIntervals(List<Interval> intervals1, List<Interval> intervals2) {
    List<Interval> res = new ArrayList<>();
    int size1 = intervals1.size();
    int size2 = intervals2.size();
    int i = 0;
    int j = 0;
    while (i < size1 || j < size2) {
      // Get the `smaller` interval
      Interval interval = null;
      if (j == size2 || i < size1 && intervals1.get(i).compareTo(intervals2.get(j)) <= 0) {
        interval = intervals1.get(i++);
      } else {
        interval = intervals2.get(j++);
      }
      // If not overlapping with current result, add as a new interval
      int resSize = res.size();
      if (res.isEmpty() || !interval.intersects(res.get(resSize - 1))) {
        res.add(interval);
      } else {
        // If overlaps with the result, union with current result
        res.set(resSize - 1, Interval.getUnion(interval, res.get(resSize - 1)));
      }
    }
    return res;
  }

  /**
   * Parse interval to millisecond as [min, max] with both sides included.
   * E.g. '(* 16311]' is parsed as [0, 16311], '(1455 16311)' is parsed as [1456, 16310]
   */
  private List<Interval> parseInterval(List<String> intervalExpressions) {
    Preconditions.checkState(intervalExpressions != null && intervalExpressions.size() == 1,
        "Cannot parse range expressions from query: %s", intervalExpressions);
    long startTime = MIN_START_TIME;
    long endTime = MAX_END_TIME;
    String intervalExpression = intervalExpressions.get(0);
    int length = intervalExpression.length();
    boolean startExclusive = intervalExpression.charAt(0) == Range.LOWER_EXCLUSIVE;
    boolean endExclusive = intervalExpression.charAt(length - 1) == Range.UPPER_EXCLUSIVE;
    String interval = intervalExpression.substring(1, length - 1);
    String[] split = StringUtils.split(interval, Range.DELIMITER);
    if (!split[0].equals(Range.UNBOUNDED)) {
      startTime = _timeFormatSpec.fromFormatToMillis(split[0]);
      if (startExclusive) {
        startTime++;
      }
    }
    if (!split[1].equals(Range.UNBOUNDED)) {
      endTime = _timeFormatSpec.fromFormatToMillis(split[1]);
      if (endExclusive) {
        endTime--;
      }
    }

    if (startTime > endTime) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new Interval(startTime, endTime));
  }
}
