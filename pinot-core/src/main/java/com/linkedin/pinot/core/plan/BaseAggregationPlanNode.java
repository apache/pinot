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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BaseAggregationPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  protected final IndexSegment indexSegment;
  protected final BrokerRequest brokerRequest;
  protected final BaseDocIdSetPlanNode docIdSetPlanNode;
  protected final BaseProjectionPlanNode projectionPlanNode;
  protected final List<BaseAggregationFunctionPlanNode> aggregationFunctionPlanNodes;

  public BaseAggregationPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    this.indexSegment = indexSegment;
    this.brokerRequest = brokerRequest;
    this.docIdSetPlanNode = getDocIdSetPlanNode();
    this.projectionPlanNode = getProjectionPlanNode();
    this.aggregationFunctionPlanNodes = getAggregationFunctionPlanNodes();
  }

  protected abstract BaseDocIdSetPlanNode getDocIdSetPlanNode();

  protected abstract BaseProjectionPlanNode getProjectionPlanNode();

  protected abstract List<BaseAggregationFunctionPlanNode> getAggregationFunctionPlanNodes();

  @Override
  public Operator run() {
    List<BAggregationFunctionOperator> aggregationFunctionOperatorList = new ArrayList<BAggregationFunctionOperator>();
    for (BaseAggregationFunctionPlanNode aggregationFunctionPlanNode : aggregationFunctionPlanNodes) {
      aggregationFunctionOperatorList.add((BAggregationFunctionOperator) aggregationFunctionPlanNode.run());
    }
    return new MAggregationOperator(indexSegment, brokerRequest.getAggregationsInfo(),
        (MProjectionOperator) projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Inner-Segment Plan Node :");
    LOGGER.debug(prefix + "Operator: MAggregationOperator");
    LOGGER.debug(prefix + "Argument 0: Projection - ");
    projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < brokerRequest.getAggregationsInfo().size(); ++i) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }
  }

  protected String[] getAggregationRelatedColumns() {
    Set<String> aggregationRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        String columns = aggregationInfo.getAggregationParams().get("column").trim();
        aggregationRelatedColumns.addAll(Arrays.asList(columns.split(",")));
      }
    }
    return aggregationRelatedColumns.toArray(new String[0]);
  }
}
