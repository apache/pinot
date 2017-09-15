/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.reduce;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.HavingFilterQuery;
import com.linkedin.pinot.common.request.HavingFilterQueryMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//This class provides a tree representation of HAVING clause predicate

public class HavingClauseComparisonTree {
  private FilterOperator _filterOperator;
  private ComparisonFunction _comparisonFunction;
  private List<HavingClauseComparisonTree> _subComparisonTree;

  public HavingClauseComparisonTree(FilterOperator filterOperator, ComparisonFunction comparisonFunction,
      List<HavingClauseComparisonTree> subComparisonTree) {
    _filterOperator = filterOperator;
    _comparisonFunction = comparisonFunction;
    _subComparisonTree = subComparisonTree;
  }

  //This functions iterates over having-filter-query and having-filter-query-map  nad build corresponding comparison tree
  public static HavingClauseComparisonTree buildHavingClauseComparisonTree(HavingFilterQuery havingFilterQuery,
      HavingFilterQueryMap havingFilterQueryMap) {
    if (havingFilterQuery.getNestedFilterQueryIdsSize() == 0) {
      return new HavingClauseComparisonTree(null, buildComparisonFunction(havingFilterQuery), null);
    } else {
      List<HavingClauseComparisonTree> subComparisonTree = new ArrayList<HavingClauseComparisonTree>();
      Iterator<Integer> iterator = havingFilterQuery.getNestedFilterQueryIdsIterator();
      while (iterator.hasNext()) {
        subComparisonTree.add(
            buildHavingClauseComparisonTree(havingFilterQueryMap.getFilterQueryMap().get(iterator.next()),
                havingFilterQueryMap));
      }
      return new HavingClauseComparisonTree(havingFilterQuery.getOperator(), null, subComparisonTree);
    }
  }

  private static ComparisonFunction buildComparisonFunction(HavingFilterQuery havingFilterQuery) {
    FilterOperator operator = havingFilterQuery.getOperator();
    ComparisonFunction comparisonFunction = null;
    if (operator == FilterOperator.EQUALITY) {
      comparisonFunction =
          new EqualComparison(havingFilterQuery.getValue().get(0), havingFilterQuery.getAggregationInfo());
    } else if (operator == FilterOperator.NOT) {
      comparisonFunction =
          new NotEqualComparison(havingFilterQuery.getValue().get(0), havingFilterQuery.getAggregationInfo());
    } else if (operator == FilterOperator.RANGE) {
      String value = havingFilterQuery.getValue().get(0);
      comparisonFunction = havingRangeStringToComparisonFunction(value, havingFilterQuery.getAggregationInfo());
    } else if (operator == FilterOperator.NOT_IN) {
      comparisonFunction =
          new InAndNotInComparison(havingFilterQuery.getValue().get(0), true, havingFilterQuery.getAggregationInfo());
    } else if (operator == FilterOperator.IN) {
      comparisonFunction =
          new InAndNotInComparison(havingFilterQuery.getValue().get(0), false, havingFilterQuery.getAggregationInfo());
    } else {
      throw new IllegalStateException("The " + operator.toString() + " operator is not supported for HAVING clause");
    }
    return comparisonFunction;
  }

  private static ComparisonFunction havingRangeStringToComparisonFunction(String rangeString,
      AggregationInfo aggregationInfo) {
    ComparisonFunction comparisonFunction = null;
    if (rangeString.matches("\\(\\*\\t\\t[-]?[0-9].*\\)")) {
      String[] tokens = rangeString.split("\\t|\\)");
      comparisonFunction = new LessThanComparison(tokens[2], aggregationInfo);
    } else if (rangeString.matches("\\(\\*\\t\\t[-]?[0-9].*\\]")) {
      String[] tokens = rangeString.split("\\t|\\]");
      comparisonFunction = new LessEqualComparison(tokens[2], aggregationInfo);
    } else if (rangeString.matches("\\([-]?[0-9].*\\t\\t\\*\\)")) {
      String[] tokens = rangeString.split("\\(|\\t");
      comparisonFunction = new GreaterThanComparison(tokens[1], aggregationInfo);
    } else if (rangeString.matches("\\[[-]?[0-9].*\\t\\t\\*\\)")) {
      String[] tokens = rangeString.split("\\[|\\t");
      comparisonFunction = new GreaterEqualComparison(tokens[1], aggregationInfo);
    } else if (rangeString.matches("\\[[-]?[0-9].*\\t\\t[-]?[0-9].*\\]")) {
      String[] tokens = rangeString.split("\\[|\\t\\t|\\]");
      comparisonFunction = new BetweenComparison(tokens[1], tokens[2], aggregationInfo);
    }
    return comparisonFunction;
  }

  public FilterOperator getFilterOperator() {
    return _filterOperator;
  }

  public ComparisonFunction getComparisonFunction() {
    return _comparisonFunction;
  }

  public List<HavingClauseComparisonTree> getSubComparisonTree() {
    return _subComparisonTree;
  }

  //This function evaluates if a group with its aggregation function results pass the Comparison Tree or Not
  public boolean isThisGroupPassPredicates(Map<String, Comparable> singleGroupAggResults) {
    if (_subComparisonTree == null || _subComparisonTree.isEmpty()) {
      if (singleGroupAggResults.get(_comparisonFunction.getFunctionExpression()) == null) {
        throw new IllegalStateException("All the columns in the HAVING clause expect to be in the input;"
            + _comparisonFunction.getFunctionExpression() + " is missing");
      }
      if (_comparisonFunction.isComparisonValid(
          singleGroupAggResults.get(_comparisonFunction.getFunctionExpression()).toString())) {
        return true;
      } else {
        return false;
      }
    } else {
      if (_filterOperator.equals(FilterOperator.AND)) {
        for (int i = 0; i < _subComparisonTree.size(); i++) {
          if (!(_subComparisonTree.get(i).isThisGroupPassPredicates(singleGroupAggResults))) {
            return false;
          }
        }
        return true;
      } else if (_filterOperator.equals(FilterOperator.OR)) {
        for (int i = 0; i < _subComparisonTree.size(); i++) {
          if ((_subComparisonTree.get(i).isThisGroupPassPredicates(singleGroupAggResults))) {
            return true;
          }
        }
        return false;
      }
    }
    return false;
  }
}
