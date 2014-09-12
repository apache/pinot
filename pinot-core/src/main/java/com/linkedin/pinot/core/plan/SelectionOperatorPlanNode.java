package com.linkedin.pinot.core.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MSelectionOperator;


public class SelectionOperatorPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final Selection _selection;
  private final IndexSegmentProjectionPlanNode _projectionPlanNode;
  private final Map<String, PlanNode> _columnarDataSourcePlanNodeMap = new HashMap<String, PlanNode>();

  public SelectionOperatorPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _selection = _brokerRequest.getSelections();
    _projectionPlanNode =
        new IndexSegmentProjectionPlanNode(_indexSegment, _brokerRequest, _indexSegment.getSegmentMetadata()
            .getTotalDocs());
    initColumnarDataSourcePlanNodeMap();
  }

  private void initColumnarDataSourcePlanNodeMap() {
    List<String> selectionColumns = _selection.getSelectionColumns();
    if ((selectionColumns.size() == 1) && (selectionColumns.get(0).equals("*"))) {
      selectionColumns.clear();
      selectionColumns.addAll(_indexSegment.getSegmentMetadata().getSchema().getColumnNames());
    }
    for (String column : selectionColumns) {
      if (_columnarDataSourcePlanNodeMap.containsKey(column)) {
        continue;
      }
      _columnarDataSourcePlanNodeMap.put(column, new ColumnarDataSourcePlanNode(_indexSegment, column,
          _projectionPlanNode));
    }

    if (_selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : _selection.getSelectionSortSequence()) {
        if (_columnarDataSourcePlanNodeMap.containsKey(selectionSort.getColumn())) {
          continue;
        }
        _columnarDataSourcePlanNodeMap.put(selectionSort.getColumn(), new ColumnarDataSourcePlanNode(_indexSegment,
            selectionSort.getColumn(), _projectionPlanNode));
      }
    }
  }

  @Override
  public Operator run() {
    Map<String, Operator> columnarDataSourceMap = new HashMap<String, Operator>();
    Operator projectionOp = _projectionPlanNode.run();
    for (String column : _columnarDataSourcePlanNodeMap.keySet()) {
      columnarDataSourceMap.put(column, _indexSegment.getDataSource(column, projectionOp));
    }
    return new MSelectionOperator(_indexSegment, _selection, projectionOp, columnarDataSourceMap);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MSelectionOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Selections - " + _brokerRequest.getSelections());
    System.out.println(prefix + "Argument 2: Projections - ");
    _projectionPlanNode.showTree(prefix + "    ");
    int i = 3;
    for (String column : _columnarDataSourcePlanNodeMap.keySet()) {
      System.out.println(prefix + "Argument " + (i++) + ": DataSourceOperator - " + column);
      _columnarDataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
    }

  }
}
