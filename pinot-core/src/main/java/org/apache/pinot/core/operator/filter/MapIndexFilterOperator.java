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
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;


/**
 * Filter operator for Map matching that internally uses JsonMatchFilterOperator or ExpressionFilterOperator.
 * This operator converts map predicates to JSON predicates and delegates filtering operations
 * to JsonMatchFilterOperator.
 */
public class MapIndexFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_MAP_INDEX";

  private final JsonMatchFilterOperator _jsonMatchOperator;
  private final ExpressionFilterOperator _expressionFilterOperator;
  private final String _columnName;
  private final String _keyName;
  private final Predicate _predicate;
  private final JsonMatchPredicate _jsonMatchPredicate;

  public MapIndexFilterOperator(IndexSegment indexSegment, Predicate predicate, QueryContext queryContext,
      int numDocs) {
    super(numDocs, false);
    _predicate = predicate;

    // Get column name and key name from function arguments
    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    if (arguments.size() != 2) {
      throw new IllegalStateException("Expected two arguments (column name and key name), found: " + arguments.size());
    }

    _columnName = arguments.get(0).getIdentifier();
    _keyName = cleanKey(String.valueOf(arguments.get(1).getLiteral()));

    // Get JSON index and create operator
    DataSource dataSource = indexSegment.getDataSource(_columnName);

    if (dataSource instanceof MapDataSource) {
      MapDataSource mapDS = (MapDataSource) dataSource;
      MapIndexReader mapIndexReader = mapDS.getMapIndex();
      if (mapIndexReader != null && useJsonIndex(_predicate.getType())) {
        Map<IndexType, IndexReader> indexes = mapIndexReader.getKeyIndexes(_keyName);
        JsonIndexReader jsonIndex = (JsonIndexReader) indexes.get(StandardIndexes.json());
        _jsonMatchPredicate = createJsonMatchPredicate();
        _jsonMatchOperator = initializeJsonMatchFilterOperator(jsonIndex, numDocs);
        _expressionFilterOperator = null;
      } else {
        _jsonMatchPredicate = null;
        _jsonMatchOperator = null;
        _expressionFilterOperator = new ExpressionFilterOperator(indexSegment, queryContext, predicate, numDocs);
      }
    } else {
      throw new IllegalStateException(
          "Expected MapDataSource for column: " + _columnName + ", found: " + dataSource.getClass().getSimpleName());
    }
  }

  /**
   * Creates a JsonMatchPredicate based on the original predicate type
   */
  private JsonMatchPredicate createJsonMatchPredicate() {
    // Convert predicate to JSON format based on type
    String jsonValue;
    switch (_predicate.getType()) {
      case EQ:
        jsonValue = createJsonEqPredicateValue(_keyName, ((EqPredicate) _predicate).getValue());
        break;
      case NOT_EQ:
        jsonValue = createJsonNotEqPredicateValue(_keyName, ((NotEqPredicate) _predicate).getValue());
        break;
      case IN:
        jsonValue = createJsonInPredicateValue(_keyName, ((InPredicate) _predicate).getValues());
        break;
      case NOT_IN:
        jsonValue = createJsonNotInPredicateValue(_keyName, ((NotInPredicate) _predicate).getValues());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported predicate type for creating json match predicate: " + _predicate.getType());
    }

    // Create identifier expression for the JSON column
    ExpressionContext jsonLhs = ExpressionContext.forIdentifier("json");
    return new JsonMatchPredicate(jsonLhs, jsonValue);
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
   * Initializes the JsonMatchFilterOperator with the given JsonIndexReader
   */
  private JsonMatchFilterOperator initializeJsonMatchFilterOperator(JsonIndexReader jsonIndex, int numDocs) {
    return new JsonMatchFilterOperator(jsonIndex, _jsonMatchPredicate, numDocs);
  }

  private String createJsonEqPredicateValue(String key, String value) {
    return String.format("%s = %s", key, value);
  }

  private String createJsonNotEqPredicateValue(String key, String value) {
    return String.format("%s != %s", key, value);
  }

  private String createJsonInPredicateValue(String key, List<String> values) {
    StringBuilder valuesStr = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        valuesStr.append(", ");
      }
      valuesStr.append(values.get(i));
    }
    // Format: '"key" IN (value1, value2)'
    return String.format("%s IN (%s)", key, valuesStr);
  }

  private String createJsonNotInPredicateValue(String key, List<String> values) {
    StringBuilder valuesStr = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        valuesStr.append(", ");
      }
      valuesStr.append(values.get(i));
    }
    // Format: '"key" NOT IN (value1, value2)'
    return String.format("%s NOT IN (%s)", key, valuesStr);
  }

  /**
   * Cleans the key by removing leading and trailing single quotes if present.
   *
   * @param key The original key string
   * @return The cleaned key string
   */
  private static String cleanKey(String key) {
    String cleanKey = key;
    if (cleanKey.startsWith("'") && cleanKey.endsWith("'")) {
      cleanKey = cleanKey.substring(1, cleanKey.length() - 1);
    }
    return cleanKey;
  }

  /**
   * Determines whether to use JSON index for the given predicate type.
   *
   * @param predicateType The type of predicate
   * @return true if the predicate type is supported for JSON index, false otherwise
   */
  private boolean useJsonIndex(Predicate.Type predicateType) {
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
