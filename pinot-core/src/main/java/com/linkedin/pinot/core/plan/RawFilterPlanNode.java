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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.*;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.*;

import java.util.ArrayList;
import java.util.List;

public class RawFilterPlanNode extends BaseFilterPlanNode {
  public RawFilterPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    super(indexSegment, brokerRequest);
  }

  @Override
  protected Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    Operator ret = null;

    if (null == filterQueryTree) {
      return null;
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (!isLeaf) {
      List<Operator> operators = new ArrayList<Operator>();
      for (final FilterQueryTree query : childFilters) {
        Operator childOperator = constructPhysicalOperator(query);
        operators.add(childOperator);
      }
      final FilterOperator filterType = filterQueryTree.getOperator();
      switch (filterType) {
        case AND:
          reorder(operators);
          ret = new AndOperator(operators);
          break;
        case OR:
          reorder(operators);
          ret = new OrOperator(operators);
          break;
        default:
          throw new UnsupportedOperationException("Not support filter type - " + filterType
              + " with children operators");
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
        default:
          throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
      }

      DataSource ds;
      ds = indexSegment.getDataSource(column);
      DataSourceMetadata dataSourceMetadata = ds.getDataSourceMetadata();
      BaseFilterOperator baseFilterOperator;

      if (dataSourceMetadata.hasInvertedIndex()) {
        if (dataSourceMetadata.isSingleValue() && dataSourceMetadata.isSorted()) {
          //if the column is sorted use sorted inverted index based implementation
          baseFilterOperator = new SortedInvertedIndexBasedFilterOperator(ds);
        } else {
          baseFilterOperator = new BitmapBasedFilterOperator(ds);
          //baseFilterOperator = new ScanBasedFilterOperator(ds);
        }
      } else {
        baseFilterOperator = new ScanBasedFilterOperator(ds, null,
            indexSegment.getSegmentMetadata().getTotalDocs() - indexSegment.getSegmentMetadata().getTotalAggregateDocs());
      }
      baseFilterOperator.setPredicate(predicate);
      ret = baseFilterOperator;
    }
    return ret;
  }
}
