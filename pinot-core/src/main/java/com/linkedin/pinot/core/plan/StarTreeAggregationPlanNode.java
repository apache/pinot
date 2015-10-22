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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;

import java.util.ArrayList;
import java.util.List;

public class StarTreeAggregationPlanNode extends BaseAggregationPlanNode {
  private static final int DEFAULT_MAX_DOC_PER_AGGREGATION = 5000;

  public StarTreeAggregationPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    super(indexSegment, brokerRequest);
  }

  @Override
  protected BaseDocIdSetPlanNode getDocIdSetPlanNode() {
    return new StarTreeDocIdSetPlanNode(indexSegment, brokerRequest, DEFAULT_MAX_DOC_PER_AGGREGATION);
  }

  @Override
  protected BaseProjectionPlanNode getProjectionPlanNode() {
    return new RawProjectionPlanNode(indexSegment, getAggregationRelatedColumns(), docIdSetPlanNode);
  }

  @Override
  protected List<BaseAggregationFunctionPlanNode> getAggregationFunctionPlanNodes() {
    List<BaseAggregationFunctionPlanNode> aggregationFunctionPlanNodes = new ArrayList<>();
    for (int i = 0; i < brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = brokerRequest.getAggregationsInfo().get(i);
      boolean hasDictionary = AggregationFunctionUtils.isAggregationFunctionWithDictionary(aggregationInfo, indexSegment);
      aggregationFunctionPlanNodes.add(new BaseAggregationFunctionPlanNode(aggregationInfo, projectionPlanNode, hasDictionary));
    }
    return aggregationFunctionPlanNodes;
  }
}
