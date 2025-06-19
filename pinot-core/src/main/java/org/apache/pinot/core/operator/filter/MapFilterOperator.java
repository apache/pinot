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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;


/**
 * Filter operator for Map matching that internally uses JsonMatchFilterOperator or ExpressionFilterOperator.
 * This operator converts map predicates to JSON predicates and delegates filtering operations
 * to JsonMatchFilterOperator.
 */
public class MapFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_MAP";

  private final JsonMatchFilterOperator _jsonMatchOperator;
  private final ExpressionFilterOperator _expressionFilterOperator;
  private final String _columnName;
  private final String _keyName;
  private final Predicate _predicate;

  public MapFilterOperator(IndexSegment indexSegment, Predicate predicate, QueryContext queryContext,
      int numDocs) {
    super(numDocs, false);
    _predicate = predicate;

    // Get column name and key name from function arguments
    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    if (arguments.size() != 2) {
      throw new IllegalStateException("Expected two arguments (column name and key name), found: " + arguments.size());
    }

    _columnName = arguments.get(0).getIdentifier();
    _keyName = arguments.get(1).getLiteral().getStringValue();

    JsonIndexReader jsonIndex = null;
    if (canUseJsonIndex(_predicate.getType())) {
      DataSource dataSource = indexSegment.getDataSourceNullable(_columnName);
      if (dataSource != null) {
        jsonIndex = dataSource.getJsonIndex();
      }
    }
    if (jsonIndex != null) {
      FilterContext filterContext = createFilterContext();
      _jsonMatchOperator = new JsonMatchFilterOperator(jsonIndex, filterContext, numDocs);
      _expressionFilterOperator = null;
    } else {
      _jsonMatchOperator = null;
      _expressionFilterOperator = new ExpressionFilterOperator(indexSegment, queryContext, predicate, numDocs);
    }
  }

  /**
   * Creates a FilterContext based on the original predicate type
   */
  private FilterContext createFilterContext() {
    // Create identifier expression for the JSON column
    ExpressionContext keyLhs = ExpressionContext.forIdentifier(_keyName);

    // Create predicate based on type
    Predicate predicate;
    switch (_predicate.getType()) {
      case EQ:
        predicate = new EqPredicate(keyLhs, ((EqPredicate) _predicate).getValue());
        break;
      case NOT_EQ:
        predicate = new NotEqPredicate(keyLhs, ((NotEqPredicate) _predicate).getValue());
        break;
      case IN:
        predicate = new InPredicate(keyLhs, ((InPredicate) _predicate).getValues());
        break;
      case NOT_IN:
        predicate = new NotInPredicate(keyLhs, ((NotInPredicate) _predicate).getValues());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported predicate type for creating filter context: " + _predicate.getType());
    }

    return FilterContext.forPredicate(predicate);
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_jsonMatchOperator != null) {
      return _jsonMatchOperator.getTrues();
    } else {
      return _expressionFilterOperator.getTrues();
    }
  }

  @Override
  public boolean canOptimizeCount() {
    if (_jsonMatchOperator != null) {
      return _jsonMatchOperator.canOptimizeCount();
    } else {
      return _expressionFilterOperator.canOptimizeCount();
    }
  }

  @Override
  public int getNumMatchingDocs() {
    if (_jsonMatchOperator != null) {
      return _jsonMatchOperator.getNumMatchingDocs();
    } else {
      return _expressionFilterOperator.getNumMatchingDocs();
    }
  }

  @Override
  public boolean canProduceBitmaps() {
    if (_jsonMatchOperator != null) {
      return _jsonMatchOperator.canProduceBitmaps();
    } else {
      return _expressionFilterOperator.canProduceBitmaps();
    }
  }

  @Override
  public BitmapCollection getBitmaps() {
    if (_jsonMatchOperator != null) {
      return _jsonMatchOperator.getBitmaps();
    } else {
      return _expressionFilterOperator.getBitmaps();
    }
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder =
        new StringBuilder(EXPLAIN_NAME).append("(column:").append(_columnName).append(",key:").append(_keyName)
            .append(",indexLookUp:map_index").append(",operator:").append(_predicate.getType()).append(",predicate:")
            .append(_predicate);

    if (_jsonMatchOperator != null) {
      stringBuilder.append(",delegateTo:json_match");
    } else {
      stringBuilder.append(",delegateTo:expression_filter");
    }

    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("column", _columnName);
    attributeBuilder.putString("key", _keyName);
    attributeBuilder.putString("indexLookUp", "map_index");
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("predicate", _predicate.toString());

    if (_jsonMatchOperator != null) {
      attributeBuilder.putString("delegateTo", "json_match");
    } else {
      attributeBuilder.putString("delegateTo", "expression_filter");
    }
  }

  /**
   * Determines whether to use JSON index for the given predicate type.
   *
   * @param predicateType The type of predicate
   * @return true if the predicate type is supported for JSON index, false otherwise
   */
  private static boolean canUseJsonIndex(Predicate.Type predicateType) {
    switch (predicateType) {
      case EQ:
      case NOT_EQ:
      case IN:
      case NOT_IN:
        return true;
      default:
        return false;
    }
  }
}
