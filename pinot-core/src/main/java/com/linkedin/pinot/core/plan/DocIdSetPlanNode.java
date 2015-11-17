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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;


/**
 * DocIdSetPlanNode takes care creating BDocIdSetOperator.
 * Configure filter query and max size of docId cache here.
 *
 *
 * This logic is refactored into another class used by {@link com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV3}
 * @see RawDocIdSetPlanNode
 */
@Deprecated
public class DocIdSetPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocIdSetPlanNode.class);
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;
  private final int _maxDocPerAggregation;
  private BReusableFilteredDocIdSetOperator _projectOp = null;

  public DocIdSetPlanNode(IndexSegment indexSegment, BrokerRequest query, int maxDocPerAggregation) {
    _maxDocPerAggregation = maxDocPerAggregation;
    _indexSegment = indexSegment;
    _brokerRequest = query;
    if (_brokerRequest.isSetFilterQuery()) {
      _filterNode = new FilterPlanNode(_indexSegment, _brokerRequest);
    } else {
      _filterNode = null;
    }
  }

  @Override
  public Operator run() {
    int totalRawDocs = _indexSegment.getTotalDocs() - _indexSegment.getSegmentMetadata().getTotalAggregateDocs();
    long start = System.currentTimeMillis();
    if (_projectOp == null) {
      if (_filterNode != null) {
        _projectOp =
            new BReusableFilteredDocIdSetOperator(_filterNode.run(), totalRawDocs,
                _maxDocPerAggregation);
      } else {
        _projectOp = new BReusableFilteredDocIdSetOperator(null, totalRawDocs, _maxDocPerAggregation);
      }
      long end = System.currentTimeMillis();
      LOGGER.debug("DocIdSetPlanNode.run took:" + (end - start));
      return _projectOp;
    } else {
      return _projectOp;
    }

  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "DocIdSetPlanNode Plan Node :");
    LOGGER.debug(prefix + "Operator: BReusableFilteredDocIdSetOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    if (_filterNode != null) {
      LOGGER.debug(prefix + "Argument 1: FilterPlanNode :(see below)");
      _filterNode.showTree(prefix + "    ");
    }
  }

}
