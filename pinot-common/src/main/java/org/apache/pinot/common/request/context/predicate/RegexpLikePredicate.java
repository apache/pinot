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
import java.util.regex.Pattern;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Predicate for REGEXP_LIKE.
 */
public class RegexpLikePredicate extends BasePredicate {
  private final String _value;
  private Pattern _pattern = null;

  public RegexpLikePredicate(ExpressionContext lhs, String value) {
    super(lhs);
    _value = value;
  }

  public RegexpLikePredicate(ExpressionContext lhs, String value, String flag) {
    super(lhs);
    _value = value;
    switch (flag) {
      case "i":
        _pattern = Pattern.compile(_value, Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE);
        break;
      default:
        _pattern = Pattern.compile(_value);
        break;
    }
  }

  @Override
  public Type getType() {
    return Type.REGEXP_LIKE;
  }

  public String getValue() {
    return _value;
  }

  public Pattern getPattern() {
    if (_pattern == null) {
      _pattern = Pattern.compile(_value, Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE);
    }
    return _pattern;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RegexpLikePredicate)) {
      return false;
    }
    RegexpLikePredicate that = (RegexpLikePredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value);
  }

  @Override
  public String toString() {
    return "regexp_like(" + _lhs + ",'" + _value + "')";
  }
}
