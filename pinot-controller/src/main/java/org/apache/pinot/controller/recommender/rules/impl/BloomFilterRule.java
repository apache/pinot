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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.BloomFilterRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create bloomfilter for dimensions frequently used in EQ predicate
 *    The partitioned dimension should be frequently used in the “=”
 *    Skip the no dictionary columns
 */
public class BloomFilterRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterRule.class);
  private final BloomFilterRuleParams _params;
  // Derived from BloomFilterHandler
  private static final Set<DataType> COMPATIBLE_DATA_TYPES = new HashSet<>(
      Arrays.asList(DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.STRING, DataType.BYTES));

  public BloomFilterRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getBloomFilterRuleParams();
  }

  @Override
  public void run() {
    int numCols = _input.getNumCols();
    double[] weights = new double[numCols];
    AtomicDouble totalWeight = new AtomicDouble(0);

    // For each query, find out the dimensions used in 'EQ'
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
    LOGGER.debug("Weight: {}, Total {}", weights, totalWeight);

    for (int i = 0; i < numCols; i++) {
      String dimName = _input.intToColName(i);
      DataType columnType = _input.getFieldType(dimName);
      if (COMPATIBLE_DATA_TYPES.contains(columnType)
          && ((weights[i] / totalWeight.get()) > _params._thresholdMinPercentEqBloomfilter)
          //The partitioned dimension should be frequently > P used
          && (_input.getCardinality(dimName)
          < _params._thresholdMaxCardinalityBloomfilter)) { //The Cardinality < C (1 million for 1MB size)
        _output.getIndexConfig().getBloomFilterColumns().add(dimName);
      }
    }
  }

  public FixedLenBitset parseQuery(QueryContext queryContext) {
    FilterContext filter = queryContext.getFilter();
    if (filter == null || filter.isConstant()) {
      return FixedLenBitset.IMMUTABLE_EMPTY_SET;
    }

    LOGGER.trace("Parsing Where Clause: {}", filter);
    return parsePredicateList(filter);
  }

  /**
   * TODO: The partitioned dimension should used in the “=” （IN, NOT IN, != are not using bloom filter in Pinot for
   * now) filter.
   * @param filterContext filterContext
   * @return dimension used in eq in this query
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
      } else if (filterContext.getPredicate().getType() == Predicate.Type.EQ
          || filterContext.getPredicate().getType() == Predicate.Type.IN) {
        ret.add(_input.colNameToInt(colName));
      }
    }
    return ret;
  }

  private FixedLenBitset mutableEmptySet() {
    return new FixedLenBitset(_input.getNumCols());
  }
}
