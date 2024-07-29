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
package org.apache.pinot.controller.recommender.rules.impl;

import com.google.common.util.concurrent.AtomicDouble;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.RangeIndexRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create range-index for columns used in <, > and between predicate
 * Skip the inverted and sorted index columns if recommended
 */
public class RangeIndexRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexRule.class);
  private final RangeIndexRuleParams _params;

  public RangeIndexRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getRangeIndexRuleParams();
  }

  @Override
  public void run() {
    int numCols = _input.getNumCols();
    double[] weights = new double[numCols];
    AtomicDouble totalWeight = new AtomicDouble(0);

    // For each query, find out the range columns
    // and accumulate the (weighted) frequencies
    _input.getParsedQueries().forEach(query -> {
      Double weight = _input.getQueryWeight(query);
      totalWeight.addAndGet(weight);
      FixedLenBitset fixedLenBitset = parseQuery(_input.getQueryContext(query));
      LOGGER.debug("fixedLenBitset {}", fixedLenBitset);
      for (Integer i : fixedLenBitset.getOffsets()) {
        weights[i] += weight;
      }
    });
    LOGGER.info("Weight: {}, Total {}", weights, totalWeight);

    for (int i = 0; i < numCols; i++) {
      String colName = _input.intToColName(i);
      // This checks if column is not already recommended for inverted index or sorted index
      // As currently, only numeric columns are selected in range index creation, we will skip non numeric columns
      if (((weights[i] / totalWeight.get()) > _params._thresholdMinPercentRangeIndex) && !_output.getIndexConfig()
          .getSortedColumn().equals(colName) && !_output.getIndexConfig().getInvertedIndexColumns().contains(colName)
          && _input.getCardinality(colName) > _params._thresholdMinCardinalityRangeIndex
          && _input.getFieldType(colName).isNumeric()) {
        _output.getIndexConfig().getRangeIndexColumns().add(colName);
      }
    }
  }

  public FixedLenBitset parseQuery(QueryContext queryContext) {
    FilterContext filter = queryContext.getFilter();
    if (filter == null || filter.isConstant()) {
      return FixedLenBitset.IMMUTABLE_EMPTY_SET;
    }

    return parsePredicateList(filter);
  }

  /**
   * As cardinality does not matter, AND and OR columns will be considered
   * @param filterContext filterContext
   * @return FixedLenBitset for range predicates in this query
   */
  private FixedLenBitset parsePredicateList(FilterContext filterContext) {
    FixedLenBitset ret = mutableEmptySet();
    List<FilterContext> children = filterContext.getChildren();
    if (children != null) {
      // AND, OR, NOT
      for (FilterContext child : children) {
        FixedLenBitset childResult = parsePredicateList(child);
        ret.union(childResult);
      }
    } else {
      // PREDICATE
      ExpressionContext lhs = filterContext.getPredicate().getLhs();
      String colName = lhs.toString();
      if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
        LOGGER.trace("Skipping the function {}", colName);
      } else if (filterContext.getPredicate().getType() == Predicate.Type.RANGE) {
        ret.add(_input.colNameToInt(colName));
      }
    }
    return ret;
  }

  private FixedLenBitset mutableEmptySet() {
    return new FixedLenBitset(_input.getNumCols());
  }
}
