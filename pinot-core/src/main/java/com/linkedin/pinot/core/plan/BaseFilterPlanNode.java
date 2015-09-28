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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BaseFilterPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseFilterPlanNode.class);
  protected final IndexSegment indexSegment;
  protected final BrokerRequest brokerRequest;

  public BaseFilterPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    this.indexSegment = indexSegment;
    this.brokerRequest = brokerRequest;
  }

  protected abstract Operator constructPhysicalOperator(FilterQueryTree filterQueryTree);

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    Operator constructPhysicalOperator = constructPhysicalOperator(RequestUtils.generateFilterQueryTree(brokerRequest));
    long end = System.currentTimeMillis();
    LOGGER.debug("FilterPlanNode.run took:" + (end - start));
    return constructPhysicalOperator;
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure =
        prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
            + brokerRequest.getFilterQuery();
    LOGGER.debug(treeStructure);
  }

  /**
   * Re orders operators, puts Sorted -> Inverted and then Raw scan. TODO: With Inverted, we can further optimize based on cardinality
   * @param operators
   */
  protected void reorder(List<Operator> operators) {
    final Map<Operator, Integer> operatorPriorityMap = new HashMap<Operator, Integer>();
    for (Operator operator : operators) {
      Integer priority = Integer.MAX_VALUE;
      if (operator instanceof SortedInvertedIndexBasedFilterOperator) {
        priority = 0;
      } else if (operator instanceof AndOperator) {
        priority = 1;
      } else if (operator instanceof InvertedIndexBasedFilterOperator) {
        priority = 2;
      } else if (operator instanceof ScanBasedFilterOperator) {
        priority = 3;
      } else if (operator instanceof OrOperator) {
        priority = 4;
      }
      operatorPriorityMap.put(operator, priority);
    }

    Comparator<? super Operator> comparator = new Comparator<Operator>() {
      @Override
      public int compare(Operator o1, Operator o2) {
        return Integer.compare(operatorPriorityMap.get(o1), operatorPriorityMap.get(o2));
      }
    };
    Collections.sort(operators, comparator);
  }
}
