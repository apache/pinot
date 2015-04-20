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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.MSelectionOperator;


/**
 * SelectionPlanNode takes care creating MSelectionOperator.
 * @author xiafu
 *
 */
public class SelectionPlanNode implements PlanNode {
  private static final Logger _logger = Logger.getLogger(SelectionPlanNode.class);

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final Selection _selection;
  private final ProjectionPlanNode _projectionPlanNode;

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _selection = _brokerRequest.getSelections();
    int maxDocPerNextCall = 10000;

    if ((_selection.getSelectionSortSequence() == null) || _selection.getSelectionSortSequence().isEmpty()) {
      //since no ordering is required, we can just get the minimum number of docs that matches the filter criteria
      maxDocPerNextCall = Math.min(_selection.getOffset() + _selection.getSize(), maxDocPerNextCall);
    }

    DocIdSetPlanNode docIdSetPlanNode = new DocIdSetPlanNode(_indexSegment, _brokerRequest, maxDocPerNextCall);
    _projectionPlanNode =
        new ProjectionPlanNode(_indexSegment, getSelectionRelatedColumns(indexSegment), docIdSetPlanNode);
  }

  private String[] getSelectionRelatedColumns(IndexSegment indexSegment) {
    Set<String> selectionColumns = new HashSet<String>();
    selectionColumns.addAll(_selection.getSelectionColumns());
    if ((selectionColumns.size() == 1) && ((selectionColumns.toArray(new String[0]))[0].equals("*"))) {
      selectionColumns.clear();
      selectionColumns.addAll(Arrays.asList(indexSegment.getColumnNames()));
    }
    if (_selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : _selection.getSelectionSortSequence()) {
        selectionColumns.add(selectionSort.getColumn());
      }
    }
    return selectionColumns.toArray(new String[0]);
  }

  @Override
  public Operator run() {
    Map<String, Operator> columnarReaderDataSourceMap = new HashMap<String, Operator>();
    return new MSelectionOperator(_indexSegment, _selection, _projectionPlanNode.run());
  }

  @Override
  public void showTree(String prefix) {
    _logger.debug(prefix + "Inner-Segment Plan Node :");
    _logger.debug(prefix + "Operator: MSelectionOperator");
    _logger.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    _logger.debug(prefix + "Argument 1: Selections - " + _brokerRequest.getSelections());
    _logger.debug(prefix + "Argument 2: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");

  }
}
