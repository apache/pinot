package com.linkedin.pinot.core.plan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final Selection _selection;
  private final ProjectionPlanNode _projectionPlanNode;

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _selection = _brokerRequest.getSelections();

    _projectionPlanNode =
        new ProjectionPlanNode(_indexSegment, getSelectionRelatedColumns(indexSegment), new DocIdSetPlanNode(
            _indexSegment, _brokerRequest, 10000));
  }

  private String[] getSelectionRelatedColumns(IndexSegment indexSegment) {
    Set<String> selectionColumns = new HashSet<String>();
    selectionColumns.addAll(_selection.getSelectionColumns());
    if ((selectionColumns.size() == 1) && ((selectionColumns.toArray(new String[0]))[0].equals("*"))) {
      selectionColumns.clear();
      selectionColumns.addAll(indexSegment.getSegmentMetadata().getSchema().getColumnNames());
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
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MSelectionOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Selections - " + _brokerRequest.getSelections());
    System.out.println(prefix + "Argument 2: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");

  }
}
