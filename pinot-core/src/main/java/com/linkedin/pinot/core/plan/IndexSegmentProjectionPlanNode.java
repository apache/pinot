package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BIndexSegmentProjectionOperator;


public class IndexSegmentProjectionPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;
  private final int _maxDocPerAggregation;
  private BIndexSegmentProjectionOperator _projectOp = null;

  public IndexSegmentProjectionPlanNode(IndexSegment indexSegment, BrokerRequest query, int maxDocPerAggregation) {
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
  public synchronized Operator run() {
    if (_projectOp == null) {
      if (_filterNode != null) {
        _projectOp = new BIndexSegmentProjectionOperator(_filterNode.run(), _indexSegment, _maxDocPerAggregation);
      } else {
        _projectOp = new BIndexSegmentProjectionOperator(null, _indexSegment, _maxDocPerAggregation);
      }
      return _projectOp;
    } else {
      return _projectOp;
    }

  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Projection Index Segment Plan Node :");
    System.out.println(prefix + "Operator: BIndexSegmentProjectionOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    if (_filterNode != null) {
      System.out.println(prefix + "Argument 1: FilterPlanNode :(see below)");
      _filterNode.showTree(prefix + "    ");
    }
  }

}
