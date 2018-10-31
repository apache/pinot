/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
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
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
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

  public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY = "max.init.group.holder.capacity";
  public static final int DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
  public static final String NUM_GROUPS_LIMIT = "num.groups.limit";
  public static final int DEFAULT_NUM_GROUPS_LIMIT = 100_000;

  private final int _maxInitialResultHolderCapacity;
  // Limit on number of groups, beyond which no new group will be created
  private final int _numGroupsLimit;

  @VisibleForTesting
  public InstancePlanMakerImplV2() {
    _maxInitialResultHolderCapacity = DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    _numGroupsLimit = DEFAULT_NUM_GROUPS_LIMIT;
  }

  @VisibleForTesting
  public InstancePlanMakerImplV2(int maxInitialResultHolderCapacity, int numGroupsLimit) {
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
  }

  /**
   * Constructor for usage when client requires to pass {@link QueryExecutorConfig} to this class.
   * <ul>
   *   <li>Set limit on the initial result holder capacity</li>
   *   <li>Set limit on number of groups returned from each segment and combined result</li>
   * </ul>
   *
   * @param queryExecutorConfig Query executor configuration
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    _maxInitialResultHolderCapacity = queryExecutorConfig.getConfig()
        .getInt(MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY, DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _numGroupsLimit = queryExecutorConfig.getConfig().getInt(NUM_GROUPS_LIMIT, DEFAULT_NUM_GROUPS_LIMIT);
    Preconditions.checkState(_maxInitialResultHolderCapacity <= _numGroupsLimit,
        "Invalid configuration: maxInitialResultHolderCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _maxInitialResultHolderCapacity, _numGroupsLimit);
    LOGGER.info("Initializing plan maker with maxInitialResultHolderCapacity: {}, numGroupsLimit: {}",
        _maxInitialResultHolderCapacity, _numGroupsLimit);
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        return new AggregationGroupByPlanNode(indexSegment, brokerRequest, _maxInitialResultHolderCapacity,
            _numGroupsLimit);
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
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, brokerRequest, executorService, timeOutMs, _numGroupsLimit);

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
    return AggregationFunctionType.getAggregationFunctionType(aggregationInfo.getAggregationType())
        == AggregationFunctionType.COUNT;
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on dictionary.
   * It can be served through dictionary only for min, max, minmaxrange queries as of now,
   * and if a dictionary is present for the column
   * @param brokerRequest Broker request
   * @param indexSegment
   * @return True if query can be served using dictionary, false otherwise.
   */
  public static boolean isFitForDictionaryBasedPlan(BrokerRequest brokerRequest, IndexSegment indexSegment) {
    // Skipping dictionary in case of star tree. Results from dictionary won't be correct
    // because of aggregated values in metrics, and ALL value in dimension
    if ((brokerRequest.getFilterQuery() != null) || brokerRequest.isSetGroupBy() || indexSegment.getSegmentMetadata()
        .hasStarTree()) {
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
    AggregationFunctionType functionType =
        AggregationFunctionType.getAggregationFunctionType(aggregationInfo.getAggregationType());
    if (functionType.isOfType(AggregationFunctionType.MIN, AggregationFunctionType.MAX,
        AggregationFunctionType.MINMAXRANGE)) {
      String expression = AggregationFunctionUtils.getColumn(aggregationInfo);
      if (TransformExpressionTree.compileToExpressionTree(expression).isColumn()) {
        Dictionary dictionary = indexSegment.getDataSource(expression).getDictionary();
        return dictionary != null && dictionary.isSorted();
      }
    }
    return false;
  }
}
