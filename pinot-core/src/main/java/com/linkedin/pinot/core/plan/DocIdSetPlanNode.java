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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DocIdSetPlanNode takes care creating BDocIdSetOperator.
 * Configure filter query and max size of docId cache here.
 */
public class DocIdSetPlanNode implements PlanNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocIdSetPlanNode.class);
  public static int MAX_DOC_PER_CALL = 10000;
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;
  private final int _maxDocPerCall;
  private BReusableFilteredDocIdSetOperator _projectOp = null;

  public DocIdSetPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    this(indexSegment, query, MAX_DOC_PER_CALL);
  }

  /**
   * @param indexSegment
   * @param query
   * @param maxDocPerCall must be <= MAX_DOC_PER_CALL
   */
  public DocIdSetPlanNode(IndexSegment indexSegment, BrokerRequest query, int maxDocPerCall) {
    _maxDocPerCall = Math.min(maxDocPerCall, MAX_DOC_PER_CALL);
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _filterNode = new FilterPlanNode(_indexSegment, _brokerRequest);
  }

  @Override
  public Operator run() {
    int totalRawDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    long start = System.currentTimeMillis();
    if (_projectOp == null) {
      _projectOp = new BReusableFilteredDocIdSetOperator(_filterNode.run(), totalRawDocs, _maxDocPerCall);
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
