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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDocIdSetPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocIdSetPlanNode.class);
  protected final IndexSegment indexSegment;
  protected final BrokerRequest brokerRequest;
  protected final int maxDocPerAggregation;
  protected final BaseFilterPlanNode filterNode;

  private BReusableFilteredDocIdSetOperator projectOp = null;

  public BaseDocIdSetPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest, int maxDocPerAggregation) {
    this.indexSegment = indexSegment;
    this.brokerRequest = brokerRequest;
    this.maxDocPerAggregation = maxDocPerAggregation;
    this.filterNode = getFilterPlanNode();
  }

  protected abstract BaseFilterPlanNode getFilterPlanNode();

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    if (projectOp == null) {
      if (filterNode != null) {
        projectOp = new BReusableFilteredDocIdSetOperator(
            filterNode.run(),
            indexSegment.getTotalDocs(),
            maxDocPerAggregation);
      } else {
        projectOp = new BReusableFilteredDocIdSetOperator(null, indexSegment.getTotalDocs(), maxDocPerAggregation);
      }
      long end = System.currentTimeMillis();
      LOGGER.debug("DocIdSetPlanNode.run took:" + (end - start));
      return projectOp;
    } else {
      return projectOp;
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "DocIdSet Plan Node :");
    LOGGER.debug(prefix + "Operator: BReusableFilteredDocIdSetOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + indexSegment.getSegmentName());
    if (filterNode != null) {
      LOGGER.debug(prefix + "Argument 1: FilterPlanNode :(see below)");
      filterNode.showTree(prefix + "    ");
    }
  }
}
