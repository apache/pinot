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
package org.apache.pinot.core.query.reduce.filter;

import java.math.BigDecimal;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Predicate matcher.
 */
public class PredicateRowMatcher implements RowMatcher {
  private final ValueExtractor _valueExtractor;
  private final DataType _valueType;
  private final Predicate.Type _predicateType;
  private final @Nullable PredicateEvaluator _predicateEvaluator;
  private final boolean _nullHandlingEnabled;

  public PredicateRowMatcher(Predicate predicate, ValueExtractor valueExtractor, boolean nullHandlingEnabled) {
    _valueExtractor = valueExtractor;
    _valueType = _valueExtractor.getColumnDataType().toDataType();
    _predicateType = predicate.getType();
    if (_predicateType == Predicate.Type.IS_NULL || _predicateType == Predicate.Type.IS_NOT_NULL) {
      _predicateEvaluator = null;
    } else {
      _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, null, _valueType);
    }
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public boolean isMatch(Object[] row) {
    Object value = _valueExtractor.extract(row);
    if (_predicateType == Predicate.Type.IS_NULL) {
      return value == null;
    } else if (_predicateType == Predicate.Type.IS_NOT_NULL) {
      return value != null;
    }
    assert (_predicateEvaluator != null);
    if (_nullHandlingEnabled && value == null) {
      return false;
    }
    switch (_valueType) {
      case INT:
        return _predicateEvaluator.applySV((int) value);
      case LONG:
        return _predicateEvaluator.applySV((long) value);
      case FLOAT:
        return _predicateEvaluator.applySV((float) value);
      case DOUBLE:
        return _predicateEvaluator.applySV((double) value);
      case BIG_DECIMAL:
        return _predicateEvaluator.applySV((BigDecimal) value);
      case BOOLEAN:
        return _predicateEvaluator.applySV((boolean) value ? 1 : 0);
      case TIMESTAMP:
        return _predicateEvaluator.applySV(((Timestamp) value).getTime());
      case STRING:
        return _predicateEvaluator.applySV((String) value);
      case BYTES:
        return _predicateEvaluator.applySV((byte[]) value);
      default:
        throw new IllegalStateException();
    }
  }
}
