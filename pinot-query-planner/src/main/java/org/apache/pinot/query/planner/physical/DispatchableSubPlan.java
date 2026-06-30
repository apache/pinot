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
package org.apache.pinot.query.planner.physical;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.ToIntFunction;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;


/**
 * The {@code DispatchableSubPlan} is the dispatchable query execution plan from the result of
 * {@link org.apache.pinot.query.planner.logical.LogicalPlanner} and
 * {@link org.apache.pinot.query.planner.physical.PinotDispatchPlanner}.
 *
 * <p>QueryPlan should contain the necessary stage boundary information and the cross exchange information
 * for:
 * <ul>
 *   <li>dispatch individual stages to executor.</li>
 *   <li>instruction for stage executor to establish connection channels to other stages.</li>
 *   <li>instruction for encoding data blocks & transferring between stages based on partitioning scheme.</li>
 * </ul>
 */
public class DispatchableSubPlan {
  private final PairList<Integer, String> _queryResultFields;

  /**
   * Map from stage id to stage plan.
   */
  private final Map<Integer, DispatchablePlanFragment> _queryStageMap;
  private final Set<String> _tableNames;
  private final Map<String, Set<String>> _tableToUnavailableSegmentsMap;
  private final long _numSegmentsPrunedByBroker;
  private final boolean _allLeafStagesEmpty;

  public DispatchableSubPlan(PairList<Integer, String> fields,
      Map<Integer, DispatchablePlanFragment> queryStageMap,
      Set<String> tableNames, Map<String, Set<String>> tableToUnavailableSegmentsMap,
      long numSegmentsPrunedByBroker) {
    this(fields, queryStageMap, tableNames, tableToUnavailableSegmentsMap, numSegmentsPrunedByBroker, false);
  }

  public DispatchableSubPlan(PairList<Integer, String> fields,
      Map<Integer, DispatchablePlanFragment> queryStageMap,
      Set<String> tableNames, Map<String, Set<String>> tableToUnavailableSegmentsMap,
      long numSegmentsPrunedByBroker, boolean allLeafStagesEmpty) {
    _queryResultFields = fields;
    _queryStageMap = queryStageMap;
    _tableNames = tableNames;
    _tableToUnavailableSegmentsMap = tableToUnavailableSegmentsMap;
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
    _allLeafStagesEmpty = allLeafStagesEmpty;
  }

  /**
   * Get a map from stage id to stage plan.
   * @return stage plan map.
   */
  public Map<Integer, DispatchablePlanFragment> getQueryStageMap() {
    return Collections.unmodifiableMap(_queryStageMap);
  }

  private static Comparator<DispatchablePlanFragment> byStageIdComparator() {
    return Comparator.comparing(d -> d.getPlanFragment().getFragmentId());
  }

  /**
   * Get the query stages.
   *
   * The returned set is sorted by stage id.
   */
  public SortedSet<DispatchablePlanFragment> getQueryStages() {
    TreeSet<DispatchablePlanFragment> treeSet = new TreeSet<>(byStageIdComparator());
    treeSet.addAll(_queryStageMap.values());
    return treeSet;
  }

  /**
   * Get the query stages without the root stage.
   *
   * The returned set is sorted by stage id.
   */
  public SortedSet<DispatchablePlanFragment> getQueryStagesWithoutRoot() {
    SortedSet<DispatchablePlanFragment> result = getQueryStages();

    DispatchablePlanFragment root = _queryStageMap.get(0);
    if (root != null) {
      result.remove(root);
    }
    return result;
  }

  /**
   * Get the query result field.
   * @return query result field.
   */
  public PairList<Integer, String> getQueryResultFields() {
    return _queryResultFields;
  }

  /**
   * Get the table names.
   * @return table names.
   */
  public Set<String> getTableNames() {
    return _tableNames;
  }

  /**
   * Get the table to unavailable segments map
   * @return table to unavailable segments map
   */
  public Map<String, Set<String>> getTableToUnavailableSegmentsMap() {
    return _tableToUnavailableSegmentsMap;
  }

  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  public boolean isAllLeafStagesEmpty() {
    return _allLeafStagesEmpty;
  }

  /**
   * Get the estimated total number of threads that will be spawned for this query (across all stages and servers).
   */
  public int getEstimatedNumQueryThreads() {
    return getEstimatedNumQueryThreads(segment -> 1);
  }

  /**
   * Get the estimated total number of threads that will be spawned for this query (across all stages and servers),
   * weighting each leaf-stage segment by the number of work units it represents.
   *
   * <p>{@code segmentWorkUnits} maps a leaf-stage segment name to its work-unit count (default 1). A caller can
   *  return a value that more accurately
   *  reflects the real work the server will perform rather than the number of routed entries.
   */
  public int getEstimatedNumQueryThreads(ToIntFunction<String> segmentWorkUnits) {
    if (_allLeafStagesEmpty) {
      return 0;
    }
    int estimatedNumQueryThreads = 0;
    // Skip broker reduce root stage
    for (DispatchablePlanFragment stage : getQueryStagesWithoutRoot()) {
      // Non-leaf stage
      if (stage.getWorkerIdToSegmentsMap().isEmpty()) {
        estimatedNumQueryThreads += stage.getWorkerMetadataList().size();
      } else {
        // Leaf stage
        for (Map<String, List<String>> segmentsMap : stage.getWorkerIdToSegmentsMap().values()) {
          int numSegments = 0;
          for (List<String> segments : segmentsMap.values()) {
            for (String segment : segments) {
              numSegments += segmentWorkUnits.applyAsInt(segment);
            }
          }

          // The leaf stage operator itself spawns a thread for each server query request
          estimatedNumQueryThreads++;

          // TODO: this isn't entirely accurate and can be improved. One issue is that the maxExecutionThreads can be
          //       overridden in the query options and also in the server query executor configs.
          //       Another issue is that not all leaf stage combine operators use the below method to calculate
          //       the number of tasks / threads (the GroupByCombineOperator has some different logic for instance).
          estimatedNumQueryThreads += QueryMultiThreadingUtils.getNumTasksForQuery(numSegments,
              QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY);
        }
      }
    }
    return estimatedNumQueryThreads;
  }
}
