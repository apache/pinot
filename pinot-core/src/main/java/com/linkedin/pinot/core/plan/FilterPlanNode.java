package com.linkedin.pinot.core.plan;

import static com.linkedin.pinot.core.common.Predicate.Type.EQ;
import static com.linkedin.pinot.core.common.Predicate.Type.NEQ;
import static com.linkedin.pinot.core.common.Predicate.Type.RANGE;
import static com.linkedin.pinot.core.common.Predicate.Type.REGEX;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BAndOperator;
import com.linkedin.pinot.core.operator.BOrOperator;
import com.linkedin.pinot.core.operator.DataSource;


public class FilterPlanNode implements PlanNode {

  private final BrokerRequest _brokerRequest;
  private final IndexSegment _segment;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    this._segment = segment;
    this._brokerRequest = brokerRequest;
  }

  @Override
  public Operator run() {
    return constructPhysicalOperator(RequestUtils.generateFilterQueryTree(_brokerRequest));
  }

  private Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    Operator ret = null;
    List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    boolean isLeaf = childFilters == null || childFilters.isEmpty();
    List<Operator> childOperators = null;
    if (!isLeaf) {
      childOperators = new ArrayList<Operator>();
      for (FilterQueryTree query : childFilters) {
        childOperators.add(constructPhysicalOperator(query));
      }
      FilterOperator filterType = filterQueryTree.getOperator();
      switch (filterType) {
        case AND:
          ret = new BAndOperator(childOperators);
          break;
        case OR:
          ret = new BOrOperator(childOperators);
          break;
      }
    } else {
      FilterOperator filterType = filterQueryTree.getOperator();
      String column = filterQueryTree.getColumn();
      Predicate predicate = null;
      List<String> value = filterQueryTree.getValue();
      switch (filterType) {
        case EQUALITY:
          predicate = new Predicate(column, EQ, value);
          break;
        case RANGE:
          predicate = new Predicate(column, RANGE, value);
          break;
        case REGEX:
          predicate = new Predicate(column, REGEX, value);
          break;
        case NOT:
          predicate = new Predicate(column, NEQ, value);
          break;
      }
      DataSource ds;
      if (predicate != null) {
        ds = _segment.getDataSource(column, predicate);
      } else {
        ds = _segment.getDataSource(column);
      }
      ret = ds;
    }
    return ret;
  }

  @Override
  public void showTree(String prefix) {
    String treeStructure =
        prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 1: "
            + _brokerRequest.toString();
    System.out.println(treeStructure);
  }
}
