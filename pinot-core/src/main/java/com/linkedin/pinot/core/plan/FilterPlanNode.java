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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.common.predicate.RegexPredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.BAndOperator;
import com.linkedin.pinot.core.operator.filter.BOrOperator;


/**
 * Construct PhysicalOperator based on given filter query.
 * @author xiafu
 *
 */
public class FilterPlanNode implements PlanNode {
  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
  private final BrokerRequest _brokerRequest;
  private final IndexSegment _segment;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    _segment = segment;
    _brokerRequest = brokerRequest;
  }

  @Override
  public Operator run() {
    return constructPhysicalOperator(RequestUtils.generateFilterQueryTree(_brokerRequest));
  }

  private Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    Operator ret = null;

    if (null == filterQueryTree) {
      return null;
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();
    List<Operator> childOperators = null;
    if (!isLeaf) {
      childOperators = new ArrayList<Operator>();
      for (final FilterQueryTree query : childFilters) {
        childOperators.add(constructPhysicalOperator(query));
      }
      final FilterOperator filterType = filterQueryTree.getOperator();
      switch (filterType) {
        case AND:
          ret = new BAndOperator(childOperators);
          break;
        case OR:
          ret = new BOrOperator(childOperators);
          break;
      }
    } else {
      final FilterOperator filterType = filterQueryTree.getOperator();
      final String column = filterQueryTree.getColumn();
      Predicate predicate = null;
      final List<String> value = filterQueryTree.getValue();
      switch (filterType) {
        case EQUALITY:
          predicate = new EqPredicate(column, value);
          break;
        case RANGE:
          predicate = new RangePredicate(column, value);
          break;
        case REGEX:
          predicate = new RegexPredicate(column, value);
          break;
        case NOT:
          predicate = new NEqPredicate(column, value);
          break;
        case NOT_IN:
          predicate = new NotInPredicate(column, value);
          break;
        case IN:
          predicate = new InPredicate(column, value);
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
    final String treeStructure =
        prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
            + _brokerRequest.getFilterQuery();
    _logger.debug(treeStructure);
  }
}
