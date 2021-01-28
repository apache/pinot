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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Predicate for IN.
 */
public class InPredicate extends BasePredicate implements Predicate {
  private final ExpressionContext _lhs;
  private List<String> _values;

  public InPredicate(ExpressionContext lhs, List<String> values) {
    _lhs = lhs;
    _values = values;
  }

  @Override
  public Type getType() {
    return Type.IN;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  public List<String> getValues() {
    return _values;
  }

  @Override
  public void rewrite(DataType dataType) {
    List<String> castedValues = new ArrayList<>();

    // If the value specified in IN predicate is not the same after conversion to column data type, we
    // can safely remove it from the IN predicate. Such a value will never match against any value in
    // the column of specified dataType.
    //
    // Consider a predicate where an integer column is being compared to a double literal. This predicate will be
    // rewritten as specified below.
    // IN PREDICATE
    //     "intColumn IN (12, 12.1, 13.0)" rewritten to "intColumn IN (12, 13)"
    //
    // The same logic applies to value of any numerical type.
    //
    for (String value : _values) {
      BigDecimal actualValue = new BigDecimal(value);
      switch (dataType) {
        case INT: {
          BigDecimal convertedValue = new BigDecimal(actualValue.intValue());
          if (actualValue.compareTo(convertedValue) == 0) {
            castedValues.add(String.valueOf(convertedValue.intValue()));
          }
          break;
        }
        case LONG: {
          BigDecimal convertedValue = new BigDecimal(actualValue.longValue());
          if (actualValue.compareTo(convertedValue) == 0) {
            castedValues.add(String.valueOf(convertedValue.longValue()));
          }
          break;
        }
        case FLOAT: {
          BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.floatValue()));
          if (actualValue.compareTo(convertedValue) == 0) {
            castedValues.add(String.valueOf(convertedValue.floatValue()));
          }
          break;
        }
        case DOUBLE: {
          BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.doubleValue()));
          if (actualValue.compareTo(convertedValue) == 0) {
            castedValues.add(String.valueOf(convertedValue.doubleValue()));
          }
          break;
        }
      }
    }

    _values = castedValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InPredicate)) {
      return false;
    }
    InPredicate that = (InPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_values, that._values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _values);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder =
        new StringBuilder(_lhs.toString()).append(" IN ('").append(_values.get(0)).append('\'');
    int numValues = _values.size();
    for (int i = 1; i < numValues; i++) {
      stringBuilder.append(",'").append(_values.get(i)).append('\'');
    }
    return stringBuilder.append(')').toString();
  }
}
