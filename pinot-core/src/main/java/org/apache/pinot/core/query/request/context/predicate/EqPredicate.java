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
package org.apache.pinot.core.query.request.context.predicate;

import java.math.BigDecimal;
import java.util.Objects;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Predicate for EQ.
 */
public class EqPredicate extends BasePredicate implements Predicate {
  private final ExpressionContext _lhs;
  private String _value;

  public EqPredicate(ExpressionContext lhs, String value) {
    _lhs = lhs;
    _value = value;
  }

  @Override
  public Type getType() {
    return Type.EQ;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  public String getValue() {
    return _value;
  }

  @Override
  public void rewrite(DataType dataType) {
    // Consider a predicate where an integer column is being compared to a double literal. This predicate will be
    // rewritten as specified below.
    // EQ PREDICATE
    //     "intColumn = 12.1" rewritten to "FALSE" by setting _precomputed value
    //     "intColumn = 12.0"	rewritten to "intColumn = 12"
    //
    // The same logic applies to value of any numerical type.
    //
    BigDecimal actualValue = new BigDecimal(_value);
    String convertedValue = _value;
    switch (dataType) {
      case INT:
        convertedValue = String.valueOf(actualValue.intValue());
        break;
      case LONG:
        convertedValue = String.valueOf(actualValue.longValue());
        break;
      case FLOAT:
        convertedValue = String.valueOf(actualValue.floatValue());
        break;
      case DOUBLE:
        convertedValue = String.valueOf(actualValue.doubleValue());
        break;
    }

    int compared = actualValue.compareTo(new BigDecimal(convertedValue));
    if (compared != 0) {
      // This predicate will always evaluate to false; hence, there is no need to evaluate the predicate during runtime.
      _precomputed = false;
    } else {
      // We need to evaluate this predicate after converting data type.
      _value = convertedValue;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EqPredicate)) {
      return false;
    }
    EqPredicate that = (EqPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value);
  }

  @Override
  public String toString() {
    return _lhs + " = '" + _value + '\'';
  }
}
