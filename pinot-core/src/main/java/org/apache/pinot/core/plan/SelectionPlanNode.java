/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.plan;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;


/**
 * The <code>SelectionPlanNode</code> class provides the execution plan for selection query on a single segment.
 */
public class SelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final Selection _selection;
  private final TransformPlanNode _transformPlanNode;

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _selection = brokerRequest.getSelections();
    _transformPlanNode =
        new TransformPlanNode(_indexSegment, brokerRequest, collectExpressionsToTransform(indexSegment, brokerRequest));
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    TransformOperator transformOperator = _transformPlanNode.run();
    if (_selection.getSize() > 0) {
      if (_selection.getSelectionSortSequence() == null) {
        return new SelectionOnlyOperator(_indexSegment, _selection, transformOperator);
      } else {
        return new SelectionOrderByOperator(_indexSegment, _selection, transformOperator);
      }
    } else {
      return new EmptySelectionOperator(_indexSegment, _selection, transformOperator);
    }
  }

  private Set<TransformExpressionTree> collectExpressionsToTransform(IndexSegment indexSegment,
      BrokerRequest brokerRequest) {

    Set<TransformExpressionTree> expressionTrees = new LinkedHashSet<>();
    Selection selection = brokerRequest.getSelections();

    // Extract selection expressions
    List<String> selectionColumns = selection.getSelectionColumns();
    if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
      for (String column : indexSegment.getPhysicalColumnNames()) {
        expressionTrees.add(new TransformExpressionTree(new IdentifierAstNode(column)));
      }
    } else {
      for (String selectionColumn : selectionColumns) {
        expressionTrees.add(TransformExpressionTree.compileToExpressionTree(selectionColumn));
      }
    }

    // Extract order-by expressions.
    if (selection.getSize() > 0) {
      List<SelectionSort> sortSequence = selection.getSelectionSortSequence();
      if (sortSequence != null) {
        for (SelectionSort selectionSort : sortSequence) {
          String orderByColumn = selectionSort.getColumn();
          expressionTrees.add(TransformExpressionTree.compileToExpressionTree(orderByColumn));
        }
      }
    }
    return expressionTrees;
  }
}
