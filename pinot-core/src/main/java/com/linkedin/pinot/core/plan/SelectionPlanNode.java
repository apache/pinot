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
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SelectionPlanNode</code> class provides the execution plan for selection query on a single segment.
 */
public class SelectionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionPlanNode.class);

  private final IndexSegment _indexSegment;
  private final Selection _selection;
  private final ProjectionPlanNode _projectionPlanNode;

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _selection = brokerRequest.getSelections();
    int maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

    if ((_selection.getSelectionSortSequence() == null) || _selection.getSelectionSortSequence().isEmpty()) {
      //since no ordering is required, we can just get the minimum number of docs that matches the filter criteria
      maxDocPerNextCall = Math.min(_selection.getOffset() + _selection.getSize(), maxDocPerNextCall);
    }

    DocIdSetPlanNode docIdSetPlanNode = new DocIdSetPlanNode(_indexSegment, brokerRequest, maxDocPerNextCall);
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment,
        SelectionOperatorUtils.extractSelectionRelatedColumns(_selection, indexSegment), docIdSetPlanNode);
  }

  @Override
  public Operator run() {
    // Use selection order-by operator only if there are sorting columns and selection size is not 0.
    if (_selection.isSetSelectionSortSequence() && (_selection.getSize() != 0)) {
      return new MSelectionOrderByOperator(_indexSegment, _selection, _projectionPlanNode.run());
    } else {
      return new MSelectionOnlyOperator(_indexSegment, _selection, _projectionPlanNode.run());
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    if (_selection.isSetSelectionSortSequence()) {
      LOGGER.debug(prefix + "Operator: MSelectionOrderByOperator");
    } else {
      LOGGER.debug(prefix + "Operator: MSelectionOnlyOperator");
    }
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Selections - " + _selection);
    LOGGER.debug(prefix + "Argument 2: Projection -");
    _projectionPlanNode.showTree(prefix + "    ");

  }
}
