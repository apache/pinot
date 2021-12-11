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
package org.apache.pinot.common.request.context.predicate;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Predicate for NOT_IN.
 */
public class NotInPredicate implements Predicate {
  private final ExpressionContext _lhs;
  private final List<String> _values;

  public NotInPredicate(ExpressionContext lhs, List<String> values) {
    _lhs = lhs;
    _values = values;
  }

  @Override
  public Type getType() {
    return Type.NOT_IN;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  public List<String> getValues() {
    return _values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NotInPredicate)) {
      return false;
    }
    NotInPredicate that = (NotInPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_values, that._values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _values);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder =
        new StringBuilder(_lhs.toString()).append(" NOT IN ('").append(_values.get(0)).append('\'');
    int numValues = _values.size();
    for (int i = 1; i < numValues; i++) {
      stringBuilder.append(",'").append(_values.get(i)).append('\'');
    }
    return stringBuilder.append(')').toString();
  }
}
