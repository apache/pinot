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

import java.util.Objects;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Predicate for TEXT_MATCH.
 */
public class TextMatchPredicate implements Predicate {
  private final ExpressionContext _lhs;
  private final String _value;

  public TextMatchPredicate(ExpressionContext lhs, String value) {
    _lhs = lhs;
    _value = value;
  }

  @Override
  public Type getType() {
    return Type.TEXT_MATCH;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  public String getValue() {
    return _value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextMatchPredicate)) {
      return false;
    }
    TextMatchPredicate that = (TextMatchPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value);
  }

  @Override
  public String toString() {
    return "text_match(" + _lhs + ",'" + _value + "')";
  }
}
