/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.groupby;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for creating GroupByExecutor of different types.
 */
public class GroupByExecutorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByExecutor.class);

  /**
   * Returns the appropriate implementation of GroupByExecutor based on
   * pre-defined criteria.
   *
   * Returns DefaultGroupByExecutor currently. May return other implementations
   * in future, if necessary.
   *
   * @param indexSegment
   * @param aggregationInfoList
   * @param groupBy
   * @return
   */
  public static GroupByExecutor getGroupByExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      GroupBy groupBy) {
    int maxNumGroupKeys = computeMaxNumGroupKeys(indexSegment, groupBy);
    return new DefaultGroupByExecutor(indexSegment, aggregationInfoList, groupBy, maxNumGroupKeys);
  }

  /**
   * Computes and returns the product of cardinality of all group-by columns.
   * If product cannot fit into an integer, or if any of the group-by columns
   * is a multi-valued column, returns Integer.MAX_VALUE.
   *
   * @param indexSegment
   * @param groupBy
   * @return
   */
  private static int computeMaxNumGroupKeys(IndexSegment indexSegment, GroupBy groupBy) {
    int maxGroupKeys = 1;

    for (String column : groupBy.getColumns()) {
      DataSource dataSource = indexSegment.getDataSource(column);
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();

      // For multi-valued group by columns, return max.
      if (!dataSourceMetadata.isSingleValue()) {
        return Integer.MAX_VALUE;
      }

      int cardinality = dataSourceMetadata.cardinality();

      // If zero/negative cardinality found (due to bad segment), log and continue. The group by executor
      // will return an empty block and the bad segment will be handled gracefully.
      if (cardinality <= 0) {
        LOGGER.error("Zero/Negative cardinality for column {} in segment {}", column, indexSegment.getSegmentName());
        cardinality = 1;
      }

      // Protecting against overflow.
      if (maxGroupKeys > (Integer.MAX_VALUE / cardinality)) {
        return Integer.MAX_VALUE;
      }
      maxGroupKeys *= cardinality;
    }

    return maxGroupKeys;
  }
}
