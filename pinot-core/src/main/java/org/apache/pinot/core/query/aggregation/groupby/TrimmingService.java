package org.apache.pinot.core.query.aggregation.groupby;

import com.google.common.collect.MinMaxPriorityQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.operator.GroupByRow;
import org.apache.pinot.core.operator.OrderByDefn;
import org.apache.pinot.core.operator.OrderByExecutor;


public class TrimmingService {

  private int _trimSize;
  private int _trimThreshold;
  private List<OrderByDefn> _orderByDefns = null;

  public TrimmingService(int interSegmentNumGroupsLimit, long topN, List<OrderByDefn> orderByDefns) {
    _trimSize = Math.max(Math.min(interSegmentNumGroupsLimit, (int) topN) * 5, 5000);
    _trimThreshold = _trimSize * 4;
    _orderByDefns = orderByDefns;
  }

  public List<GroupByRow> trim(List<GroupByRow> groupByRows) {

    // within threshold, no need to trim
    if (groupByRows.size() <= _trimThreshold) {
      return groupByRows;
    }

    // greater than trim threshold, needs trimming
    List<GroupByRow> trimmedRows = new ArrayList<>();
    if (_orderByDefns != null) {
      // if order by, sort by that order
      OrderByExecutor orderByExecutor = new OrderByExecutor();
      Comparator<GroupByRow> comparator = orderByExecutor.getComparator(_orderByDefns);

      MinMaxPriorityQueue<GroupByRow> priorityQueue =
          MinMaxPriorityQueue.orderedBy(comparator).maximumSize(_trimSize).create();
      for (GroupByRow row : groupByRows) {
        priorityQueue.add(row);
      }
      trimmedRows.addAll(priorityQueue);
    } else {
      trimmedRows = groupByRows.subList(0, _trimSize);
    }
    return trimmedRows;
  }
}
