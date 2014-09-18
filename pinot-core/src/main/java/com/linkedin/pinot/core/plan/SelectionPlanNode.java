package com.linkedin.pinot.core.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private final DocIdSetPlanNode _docIdSetPlanNode;
  private final Map<String, PlanNode> _columnarDataSourcePlanNodeMap = new HashMap<String, PlanNode>();

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _selection = _brokerRequest.getSelections();
    _docIdSetPlanNode = new DocIdSetPlanNode(_indexSegment, _brokerRequest, 10000);
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
          _docIdSetPlanNode));
    }

    if (_selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : _selection.getSelectionSortSequence()) {
        if (_columnarDataSourcePlanNodeMap.containsKey(selectionSort.getColumn())) {
          continue;
        }
        _columnarDataSourcePlanNodeMap.put(selectionSort.getColumn(), new ColumnarDataSourcePlanNode(_indexSegment,
            selectionSort.getColumn(), _docIdSetPlanNode));
      }
    }
  }

  @Override
  public Operator run() {
    Map<String, Operator> columnarReaderDataSourceMap = new HashMap<String, Operator>();
    Operator docIdSetOperator = _docIdSetPlanNode.run();
    for (String column : _columnarDataSourcePlanNodeMap.keySet()) {
      columnarReaderDataSourceMap.put(column, _columnarDataSourcePlanNodeMap.get(column).run());
    }
    return new MSelectionOperator(_indexSegment, _selection, docIdSetOperator, columnarReaderDataSourceMap);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MSelectionOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Selections - " + _brokerRequest.getSelections());
    System.out.println(prefix + "Argument 2: DocIdSet - ");
    _docIdSetPlanNode.showTree(prefix + "    ");
    int i = 3;
    for (String column : _columnarDataSourcePlanNodeMap.keySet()) {
      System.out.println(prefix + "Argument " + (i++) + ": DataSourceOperator");
      _columnarDataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
    }

  }
}
