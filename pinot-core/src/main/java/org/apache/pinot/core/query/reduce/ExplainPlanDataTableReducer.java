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
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.ExplainPlanRowData;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExplainPlanDataTableReducer implements DataTableReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExplainPlanDataTableReducer.class);
  public static final String COMBINE = "COMBINE";

  private final QueryContext _queryContext;

  ExplainPlanDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {

    List<Object[]> reducedRows = new ArrayList<>();

    // Top node should be a BROKER_REDUCE node.
    addBrokerReduceOperation(reducedRows);

    // Construct the combine node
    Object[] combinedRow = extractCombineNode(dataTableMap);
    if (combinedRow != null) {
      reducedRows.add(combinedRow);
    }

    // Extract the query option denoting whether to return verbose output or not
    Map<String, String> queryOptions = _queryContext.getQueryOptions();
    boolean explainPlanVerbose = queryOptions != null && QueryOptionsUtils.isExplainPlanVerbose(queryOptions);

    // Add the rest of the rows for each unique Explain plan received from the servers
    List<ExplainPlanRows> explainPlanRowsList = extractUniqueExplainPlansAcrossServers(dataTableMap, combinedRow);
    if (!explainPlanVerbose && (explainPlanRowsList.size() > 1)) {
      // Pick the most appropriate plan if verbose option is disabled
      explainPlanRowsList = chooseBestExplainPlanToUse(explainPlanRowsList);
    }

    // Construct the explain plan output
    for (ExplainPlanRows explainPlanRows : explainPlanRowsList) {
      String numSegmentsExplainString = String.format(ExplainPlanRows.PLAN_START_FORMAT,
          explainPlanRows.getNumSegmentsMatchingThisPlan());
      Object[] numSegmentsRow = {numSegmentsExplainString, ExplainPlanRows.PLAN_START_IDS,
          ExplainPlanRows.PLAN_START_IDS};
      reducedRows.add(numSegmentsRow);

      for (ExplainPlanRowData explainPlanRowData : explainPlanRows.getExplainPlanRowData()) {
        Object[] row = {explainPlanRowData.getExplainPlanString(), explainPlanRowData.getOperatorId(),
            explainPlanRowData.getParentId()};
        reducedRows.add(row);
      }
    }

    ResultTable resultTable = new ResultTable(dataSchema, reducedRows);
    brokerResponseNative.setResultTable(resultTable);
  }

  /**
   * Extract the combine node to use as the global combine step if present. If no combine node is found, return null.
   * A combine node may not be found if all segments were pruned across all servers.
   */
  private Object[] extractCombineNode(Map<ServerRoutingInstance, DataTable> dataTableMap) {
    if (dataTableMap.isEmpty()) {
      return null;
    }

    Object[] combineRow = null;
    for (Map.Entry<ServerRoutingInstance, DataTable> entry : dataTableMap.entrySet()) {
      DataTable dataTable = entry.getValue();
      int numRows = dataTable.getNumberOfRows();
      if (numRows > 0) {
        // First row should be the combine row data, unless all segments were pruned from the Server side
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, 0);
        String rowName = row[0].toString();
        if (rowName.contains(COMBINE)) {
          combineRow = row;
          break;
        }
      }
    }
    return combineRow;
  }

  /**
   * Extract a list of all the unique explain plans across all servers
   */
  private List<ExplainPlanRows> extractUniqueExplainPlansAcrossServers(
      Map<ServerRoutingInstance, DataTable> dataTableMap, Object[] combinedRow) {
    List<ExplainPlanRows> explainPlanRowsList = new ArrayList<>();
    HashSet<Integer> explainPlanHashCodeSet = new HashSet<>();

    for (Map.Entry<ServerRoutingInstance, DataTable> entry : dataTableMap.entrySet()) {
      DataTable dataTable = entry.getValue();
      int numRows = dataTable.getNumberOfRows();

      ExplainPlanRows explainPlanRows = null;
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        String rowName = row[0].toString();
        if (rowName.contains(COMBINE)) {
          // Skip the combine explain plan nodes as we construct a global one for all the plans returned
          continue;
        } else if (rowName.contains(ExplainPlanRows.PLAN_START)) {
          // There may be an ongoing explain plan being constructed. NUM_MATCHING_SEGMENTS is a delimiter for the
          // various explain plans returned by each server. Thus complete the explain plan for the previous plan and
          // start a new one.
          updateExplainPlanRowsList(explainPlanRowsList, explainPlanHashCodeSet, explainPlanRows);
          explainPlanRows = new ExplainPlanRows();
          String numSegmentsString =
              StringUtils.substringBefore(StringUtils.substringAfter(rowName, ExplainPlanRows.PLAN_START), ")");
          explainPlanRows.setNumSegmentsMatchingThisPlan(Integer.parseInt(numSegmentsString));
          continue;
        }

        if (explainPlanRows == null) {
          explainPlanRows = new ExplainPlanRows();
        }

        if (rowName.contains(EmptyFilterOperator.EXPLAIN_NAME)) {
          explainPlanRows.setHasEmptyFilter(true);
        } else if (rowName.contains(MatchAllFilterOperator.EXPLAIN_NAME)) {
          explainPlanRows.setHasMatchAllFilter(true);
        } else if (rowName.contains(ExplainPlanRows.ALL_SEGMENTS_PRUNED_ON_SERVER)) {
          explainPlanRows.setHasNoMatchingSegment(true);
        }
        if (combinedRow == null && rowName.contains(ExplainPlanRows.ALL_SEGMENTS_PRUNED_ON_SERVER)) {
          // If there is no COMBINE node it means all segments got pruned across all servers. Fix up the operator ID
          // and parent ID to reflect the correct values without COMBINE
          explainPlanRows.getExplainPlanRowData().add(new ExplainPlanRowData(rowName, 2, 1));
        } else {
          explainPlanRows.getExplainPlanRowData().add(new ExplainPlanRowData(rowName, (int) row[1], (int) row[2]));
        }
      }
      // The last plan needs to be completed so that it isn't lost. There is no end delimiter, just use end of rows
      // as the end delimiter
      updateExplainPlanRowsList(explainPlanRowsList, explainPlanHashCodeSet, explainPlanRows);
    }

    // Sorting the list of explain plans to ensure we get a deterministic ordering of the plans
    Collections.sort(explainPlanRowsList);
    return explainPlanRowsList;
  }

  /**
   * Finalize and add a new explain plan to the explain plan list. Also dedupe duplicate explain plans
   */
  private void updateExplainPlanRowsList(List<ExplainPlanRows> explainPlanRowsList,
      HashSet<Integer> explainPlanHashCodeSet, ExplainPlanRows explainPlanRows) {
    if (explainPlanRows != null) {
      int explainPlanRowsHashCode = explainPlanRows.hashCode();
      if (!explainPlanHashCodeSet.contains(explainPlanRowsHashCode)) {
        // Hashcode found is unique, add to the list of explain plan nodes
        explainPlanRowsList.add(explainPlanRows);
        explainPlanHashCodeSet.add(explainPlanRowsHashCode);
      } else {
        boolean explainPlanMatchFound = false;
        for (ExplainPlanRows planRows : explainPlanRowsList) {
          if ((planRows.hashCode() == explainPlanRowsHashCode) && (planRows.equals(explainPlanRows))) {
            // If the hashcodes match and the plans are equal, update the number of segments matching this plan
            int numSegments = planRows.getNumSegmentsMatchingThisPlan();
            numSegments += explainPlanRows.getNumSegmentsMatchingThisPlan();
            planRows.setNumSegmentsMatchingThisPlan(numSegments);
            explainPlanMatchFound = true;
            break;
          }
        }

        // The hashcodes match but the plans aren't equal. HashCodes can cause collisions. Append this plan to the
        // list of plans
        if (!explainPlanMatchFound) {
          explainPlanRowsList.add(explainPlanRows);
        }
      }
    }
  }

  /**
   * Choose the best explain plan to use when verbose mode is disabled. The precedence ordering is:
   * - Other plan > match All plan > empty plan > no matching segment plan
   * - Tree depth is used if more than 1 plan exists for the winning plan type from above
   */
  private List<ExplainPlanRows> chooseBestExplainPlanToUse(List<ExplainPlanRows> explainPlanRowsList) {
    int maxOtherDepth = -1;
    int maxEmptyFilterDepth = -1;
    int maxMatchAllFilterDepth = -1;
    int maxNoMatchingSegmentDepth = -1;
    int maxOtherIdx = -1;
    int maxEmptyFilterIdx = -1;
    int maxMatchAllFilterIdx = -1;
    int maxNoMatchingSegmentIdx = -1;

    for (int i = 0; i < explainPlanRowsList.size(); i++) {
      ExplainPlanRows explainPlanRows = explainPlanRowsList.get(i);
      int explainPlanRowsSize = explainPlanRows.getExplainPlanRowData().size();
      if (explainPlanRows.isHasNoMatchingSegment()) {
        if (explainPlanRowsSize > maxNoMatchingSegmentDepth) {
          maxNoMatchingSegmentDepth = explainPlanRowsSize;
          maxNoMatchingSegmentIdx = i;
        }
      } else if (explainPlanRows.isHasEmptyFilter()) {
        if (explainPlanRowsSize > maxEmptyFilterDepth) {
          maxEmptyFilterDepth = explainPlanRowsSize;
          maxEmptyFilterIdx = i;
        }
      } else if (explainPlanRows.isHasMatchAllFilter()) {
        if (explainPlanRowsSize > maxMatchAllFilterDepth) {
          maxMatchAllFilterDepth = explainPlanRowsSize;
          maxMatchAllFilterIdx = i;
        }
      } else {
        if (explainPlanRowsSize > maxOtherDepth) {
          maxOtherDepth = explainPlanRowsSize;
          maxOtherIdx = i;
        }
      }
    }

    // Precedence: Other > MatchAllFilter > EmptyFilter > NoMatchingSegment
    if (maxOtherIdx > -1) {
      return Collections.singletonList(explainPlanRowsList.get(maxOtherIdx));
    } else if (maxMatchAllFilterIdx > -1) {
      return Collections.singletonList(explainPlanRowsList.get(maxMatchAllFilterIdx));
    } else if (maxEmptyFilterIdx > -1) {
      return Collections.singletonList(explainPlanRowsList.get(maxEmptyFilterIdx));
    }

    return Collections.singletonList(explainPlanRowsList.get(maxNoMatchingSegmentIdx));
  }

  private void addBrokerReduceOperation(List<Object[]> resultRows) {

    Set<String> postAggregations = new HashSet<>();
    QueryContextUtils.collectPostAggregations(_queryContext, postAggregations);
    StringBuilder stringBuilder = new StringBuilder("BROKER_REDUCE").append('(');

    if (_queryContext.getHavingFilter() != null) {
      stringBuilder.append("havingFilter").append(':').append(_queryContext.getHavingFilter().toString()).append(',');
    }

    if (_queryContext.getOrderByExpressions() != null) {
      stringBuilder.append("sort").append(':').append(_queryContext.getOrderByExpressions().toString()).append(',');
    }

    stringBuilder.append("limit:").append(_queryContext.getLimit());
    if (!postAggregations.isEmpty()) {
      stringBuilder.append(",postAggregations:");
      int count = 0;
      for (String func : postAggregations) {
        if (count == postAggregations.size() - 1) {
          stringBuilder.append(func);
        } else {
          stringBuilder.append(func).append(", ");
        }
        count++;
      }
    }

    String brokerReduceNode = stringBuilder.append(')').toString();
    Object[] brokerReduceRow = new Object[]{brokerReduceNode, 1, 0};

    resultRows.add(brokerReduceRow);
  }
}
