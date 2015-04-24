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
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.common.predicate.RegexPredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.BBitmapOnlyAndOperator;
import com.linkedin.pinot.core.operator.filter.BBitmapOnlyOrOperator;
import com.linkedin.pinot.core.operator.filter.MAndOperator;
import com.linkedin.pinot.core.operator.filter.MOrOperator;


/**
 * Construct PhysicalOperator based on given filter query.
 * @author xiafu
 *
 */
public class FilterPlanNode implements PlanNode {
  private static final Logger _logger = Logger.getLogger(FilterPlanNode.class);
  private final BrokerRequest _brokerRequest;
  private final IndexSegment _segment;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    _segment = segment;
    _brokerRequest = brokerRequest;
  }

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    Operator constructPhysicalOperator =
        constructPhysicalOperator(RequestUtils.generateFilterQueryTree(_brokerRequest));
    long end = System.currentTimeMillis();
    _logger.debug("FilterPlanNode.run took:" + (end - start));
    return constructPhysicalOperator;
  }

  private Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    Operator ret = null;

    if (null == filterQueryTree) {
      return null;
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (!isLeaf) {
      List<Operator> scanBasedOperators = new ArrayList<Operator>();
      List<Operator> invertedIndexRelatedOperators = new ArrayList<Operator>();
      for (final FilterQueryTree query : childFilters) {
        Operator childOperator = constructPhysicalOperator(query);
        if (isOpearatorInvertedIndexInvolved(childOperator)) {
          invertedIndexRelatedOperators.add(childOperator);
        } else {
          scanBasedOperators.add(childOperator);
        }
      }
      final FilterOperator filterType = filterQueryTree.getOperator();
      switch (filterType) {
        case AND:
          if (0 == scanBasedOperators.size()) {
            ret = new BBitmapOnlyAndOperator(invertedIndexRelatedOperators);
          } else {
            Operator bitMapAndOperator = new BBitmapOnlyAndOperator(invertedIndexRelatedOperators);
            scanBasedOperators.set(scanBasedOperators.size(), scanBasedOperators.get(0));
            scanBasedOperators.set(0, bitMapAndOperator);
            ret = new MAndOperator(scanBasedOperators);
          }
          break;
        case OR:
          if (0 == scanBasedOperators.size()) {
            ret = new BBitmapOnlyOrOperator(invertedIndexRelatedOperators);
          } else {
            Operator bitMapOrOperator = new BBitmapOnlyOrOperator(invertedIndexRelatedOperators);
            scanBasedOperators.set(scanBasedOperators.size(), scanBasedOperators.get(0));
            scanBasedOperators.set(0, bitMapOrOperator);
            ret = new MOrOperator(scanBasedOperators);
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support filter type - " + filterType + " with children operators");
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

  private boolean isOpearatorInvertedIndexInvolved(Operator operator) {
    if (operator instanceof BBitmapOnlyAndOperator || operator instanceof BBitmapOnlyOrOperator) {
      return true;
    } else if (operator instanceof DataSource) {
      DataSourceMetadata dsm = ((DataSource) operator).getDataSourceMetadata();
      if (dsm.hasInvertedIndex()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure =
        prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
            + _brokerRequest.getFilterQuery();
    _logger.debug(treeStructure);
  }
}
