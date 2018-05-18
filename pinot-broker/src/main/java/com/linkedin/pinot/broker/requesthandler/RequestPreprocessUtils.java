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
package com.linkedin.pinot.broker.requesthandler;

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.operator.transform.function.ValueInTransformFunction;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


public class RequestPreprocessUtils {

  /**
   * Attach VALUE_IN UDF to multi-valued columns in "filterGroupsForMVColumns" query option.
   * <p>To attach VALUE_IN UDF, the column must have one single IN filter and must be in the GROUP BY clause.
   *
   * @param brokerRequest Broker request to attach VALUE_IN UDF
   */
  public static void attachValueInUDF(@Nonnull BrokerRequest brokerRequest) {
    Map<String, String> queryOptions = brokerRequest.getQueryOptions();
    if (queryOptions == null) {
      return;
    }
    String value = queryOptions.remove(CommonConstants.Broker.Request.QueryOptionKey.ATTACH_VALUE_IN_FOR_COLUMNS);
    if (queryOptions.isEmpty()) {
      brokerRequest.unsetQueryOptions();
    }
    if (value == null || value.isEmpty()) {
      return;
    }
    FilterQueryMap filterQueryMap = brokerRequest.getFilterSubQueryMap();
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (filterQueryMap == null || groupBy == null) {
      throw new RuntimeException("Cannot attach VALUE_IN UDF to query without filter or group-by");
    }

    List<String> columns = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(value);
    int numColumns = columns.size();
    FilterQuery[] filterQueries = new FilterQuery[numColumns];
    for (FilterQuery filterQuery : filterQueryMap.getFilterQueryMap().values()) {
      String column = filterQuery.getColumn();
      if (column != null) {
        int index = columns.indexOf(column);
        if (index != -1) {
          if (filterQuery.getOperator() != FilterOperator.IN || filterQueries[index] != null) {
            throw new RuntimeException("To attach VALUE_IN UDF, column must have one single IN filter");
          }
          filterQueries[index] = filterQuery;
        }
      }
    }

    List<String> groupByColumns = groupBy.getColumns();
    List<String> groupByExpressions = groupBy.getExpressions();
    int numGroupByExpressions = groupByExpressions.size();
    for (int i = 0; i < numGroupByExpressions; i++) {
      String groupByExpression = groupByExpressions.get(i);
      int index = columns.indexOf(groupByExpression);
      if (index != -1) {
        FilterQuery filterQuery = filterQueries[index];
        groupByExpressions.set(i, TransformExpressionTree.compileToExpressionTree(
            String.format("%s(%s,'%s')", ValueInTransformFunction.FUNCTION_NAME, groupByExpression,
                String.join("','", filterQuery.getValue()))).toString());
        groupByColumns.remove(groupByExpression);
        if (groupByColumns.isEmpty()) {
          groupBy.unsetColumns();
        }
      }
    }
  }
}
