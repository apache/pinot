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
package com.linkedin.pinot.core.plan.maker;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.MetadataBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory.AggregationFunctionType;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>InstancePlanMakerImplV2</code> class is the default implementation of {@link PlanMaker}.
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);

  private static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY = "max.init.group.holder.capacity";
  private static final int DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
  private final int _maxInitialResultHolderCapacity;

  // TODO: Fix the runtime trimming and add back the number of aggregation groups limit.
  // TODO: Need to revisit the runtime trimming solution. Current solution will remove group keys that should not be removed.
  // Limit on number of groups, beyond which results are truncated.
  // private static final String NUM_AGGR_GROUPS_LIMIT = "num.aggr.groups.limit";
  // private static final int DEFAULT_NUM_AGGR_GROUPS_LIMIT = 100_000;
  private final int _numAggrGroupsLimit = Integer.MAX_VALUE;

  /**
   * Default constructor.
   */
  public InstancePlanMakerImplV2() {
    _maxInitialResultHolderCapacity = DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
//    _numAggrGroupsLimit = DEFAULT_NUM_AGGR_GROUPS_LIMIT;
  }

  /**
   * Constructor for usage when client requires to pass {@link QueryExecutorConfig} to this class.
   * <ul>
   *   <li>Set limit on number of aggregation groups in query result.</li>
   * </ul>
   *
   * @param queryExecutorConfig query executor configuration.
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    _maxInitialResultHolderCapacity = queryExecutorConfig.getConfig()
        .getInt(MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY, DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);

    // TODO: Read the limit on number of aggregation groups in query result from config.
    // _numAggrGroupsLimit = queryExecutorConfig.getConfig().getInt(NUM_AGGR_GROUPS_LIMIT, DEFAULT_NUM_AGGR_GROUPS_LIMIT);
    // LOGGER.info("Maximum number of allowed groups for group-by query results: '{}'", _numAggrGroupsLimit);
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {

      if (brokerRequest.isSetGroupBy()) {
        return new AggregationGroupByPlanNode(indexSegment, brokerRequest,
            _maxInitialResultHolderCapacity, _numAggrGroupsLimit);
      } else {
        if (isFitForMetadataBasedPlan(brokerRequest, indexSegment)) {
          return new MetadataBasedAggregationPlanNode(indexSegment, brokerRequest.getAggregationsInfo());
        } else if (isFitForDictionaryBasedPlan(brokerRequest, indexSegment)) {
          return new DictionaryBasedAggregationPlanNode(indexSegment, brokerRequest.getAggregationsInfo());
        } else {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        }
      }
    }
    if (brokerRequest.isSetSelections()) {
      return new SelectionPlanNode(indexSegment, brokerRequest);
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection.");
  }

  @Override
  public Plan makeInterSegmentPlan(List<SegmentDataManager> segmentDataManagers, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    // TODO: pass in List<IndexSegment> directly.
    List<IndexSegment> indexSegments = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      indexSegments.add(segmentDataManager.getSegment());
    }
    BrokerRequestPreProcessor.preProcess(indexSegments, brokerRequest);

    List<PlanNode> planNodes = new ArrayList<>();
    for (IndexSegment indexSegment : indexSegments) {
      planNodes.add(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, brokerRequest, executorService, timeOutMs);

    return new GlobalPlanImplV0(new InstanceResponsePlanNode(combinePlanNode));
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on metadata.
   * Currently count queries without any filters are supported.
   * The code for supporting max and min is also in place, but disabled
   * It would have worked only for time columns and offline and non star tree cases.
   *
   * @param brokerRequest Broker request
   * @param indexSegment
   * @return True if query can be served using metadata, false otherwise.
   */
  public static boolean isFitForMetadataBasedPlan(BrokerRequest brokerRequest, IndexSegment indexSegment) {
    if (brokerRequest.getFilterQuery() != null || brokerRequest.isSetGroupBy()) {
      return false;
    }

    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo == null) {
      return false;
    }
    for (AggregationInfo aggInfo : aggregationsInfo) {
      if (!isMetadataBasedAggregationFunction(aggInfo, indexSegment)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isMetadataBasedAggregationFunction(AggregationInfo aggregationInfo,
      IndexSegment indexSegment) {
    AggregationFunctionType aggFuncType = AggregationFunctionType.valueOf(aggregationInfo.getAggregationType().toUpperCase());
    if (aggFuncType.equals(AggregationFunctionType.COUNT)) {
      return true;
    }

    /** This code tries to get max and min value from segment metadata
     * By default metadata is generated only for time column.
     * In case of star tree, the min max values are incorrect,
     * as those values are read from the dictionary during segment load time
     * Also, in case of realtime, this metadata is not generated at all
     * Keeping this code for reference, but not using it for now,
     * until all the edge cases are fixed
     */
    //isMetadataBasedMinMax(aggFuncType, indexSegment, aggregationInfo);
    return false;
  }

  private static boolean isMetadataBasedMinMax(AggregationFunctionType aggFuncType,
      IndexSegment indexSegment, AggregationInfo aggregationInfo) {
    // Use minValue and maxValue from metadata, in non star tree cases
    // minValue and maxValue is generated from dictionary on segment load, so it won't be correct in case of star tree
    if (aggFuncType.isOfType(AggregationFunctionType.MAX, AggregationFunctionType.MIN, AggregationFunctionType.MINMAXRANGE)
        && !indexSegment.getSegmentMetadata().hasStarTree()) {
      String column = aggregationInfo.getAggregationParams().get("column");
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) indexSegment.getSegmentMetadata();
      if (segmentMetadata == null || segmentMetadata.getColumnMetadataMap() == null) {
        return false;
      }
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata.getMaxValue() != null && columnMetadata.getMinValue() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on dictionary.
   * It can be served through dictionary only for min, max, minmaxrange queries as of now,
   * and if a dictionary is present for the column
   * @param brokerRequest Broker request
   * @param indexSegment
   * @return True if query can be served using dictionary, false otherwise.
   */
  public static boolean isFitForDictionaryBasedPlan(BrokerRequest brokerRequest,
      IndexSegment indexSegment) {
    // Skipping dictionary in case of star tree. Results from dictionary won't be correct
    // because of aggregated values in metrics, and ALL value in dimension
    if ((brokerRequest.getFilterQuery() != null) || brokerRequest.isSetGroupBy()
        || indexSegment.getSegmentMetadata().hasStarTree()) {
      return false;
    }
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo == null) {
      return false;
    }
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (!isDictionaryBasedAggregationFunction(aggregationInfo, indexSegment)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isDictionaryBasedAggregationFunction(AggregationInfo aggregationInfo,
      IndexSegment indexSegment) {
    AggregationFunctionType aggFuncType = AggregationFunctionType.valueOf(aggregationInfo.getAggregationType().toUpperCase());
    if (aggFuncType.isOfType(AggregationFunctionType.MAX, AggregationFunctionType.MIN, AggregationFunctionType.MINMAXRANGE)) {
      String column = aggregationInfo.getAggregationParams().get("column");
      DataSource dataSource = indexSegment.getDataSource(column);
      if (dataSource.getDataSourceMetadata().hasDictionary() && dataSource.getDictionary().isSorted()) {
        return true;
      }
    }
    return false;
  }

}
