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
package org.apache.pinot.core.query.reduce;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.reduce.filter.ColumnValueExtractor;
import org.apache.pinot.core.query.reduce.filter.LiteralValueExtractor;
import org.apache.pinot.core.query.reduce.filter.RowMatcher;
import org.apache.pinot.core.query.reduce.filter.RowMatcherFactory;
import org.apache.pinot.core.query.reduce.filter.ValueExtractor;
import org.apache.pinot.core.query.reduce.filter.ValueExtractorFactory;
import org.apache.pinot.core.util.GapfillUtils;


/**
 * Handler for Filter clause of GapFill.
 */
public class GapfillFilterHandler implements ValueExtractorFactory {
  private final RowMatcher _rowMatcher;
  private final DataSchema _dataSchema;
  private final Map<String, Integer> _indexes;

  public GapfillFilterHandler(FilterContext filter, DataSchema dataSchema) {
    _dataSchema = dataSchema;
    _indexes = new HashMap<>();
    for (int i = 0; i < _dataSchema.size(); i++) {
      // TODO: This won't work for certain aggregations because the column name in schema is not expression.toString().
      // TODO: Please refer to {@link PostAggregationHandler} on how to handle the index for aggregation queries.
      _indexes.put(_dataSchema.getColumnName(i), i);
    }
    // TODO: support proper null handling in GapfillFilterHandler.
    _rowMatcher = RowMatcherFactory.getRowMatcher(filter, this, false);
  }

  /**
   * Returns {@code true} if the given row matches the HAVING clause, {@code false} otherwise.
   */
  public boolean isMatch(Object[] row) {
    return _rowMatcher.isMatch(row);
  }

  /**
   * Returns a ValueExtractor based on the given expression.
   */
  @Override
  public ValueExtractor getValueExtractor(ExpressionContext expression) {
    expression = GapfillUtils.stripGapfill(expression);
    if (expression.getType() == ExpressionContext.Type.LITERAL) {
      // Literal
      return new LiteralValueExtractor(expression.getLiteral().getStringValue());
    }

    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      return new ColumnValueExtractor(_indexes.get(expression.getIdentifierName()), _dataSchema);
    } else {
      // TODO: This does not handle transform properly (e.g. colA - colB where the gapfill selects colA and colB).
      // TODO: This is handled within the PostAggregationValueExtractor, and we may also extract that out to be shared.
      return new ColumnValueExtractor(_indexes.get(expression.getFunction().toString()), _dataSchema);
    }
  }
}
