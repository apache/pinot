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
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.common.utils.regex.PatternFactory;

/**
 * Predicate for REGEXP_LIKE with optional match parameters (Oracle-style).
 */
public class RegexpLikePredicate extends BasePredicate {
  private final String _value;
  private final String _matchParameter;
  private Pattern _pattern = null;

  public RegexpLikePredicate(ExpressionContext lhs, String value) {
    this(lhs, value, "c"); // Default case-sensitive
  }

  public RegexpLikePredicate(ExpressionContext lhs, String value, String matchParameter) {
    super(lhs);
    _value = value;
    _matchParameter = matchParameter != null ? matchParameter : "c";
  }

  @Override
  public Type getType() {
    return Type.REGEXP_LIKE;
  }

  public String getValue() {
    return _value;
  }

  public String getMatchParameter() {
    return _matchParameter;
  }

  public Pattern getPattern() {
    if (_pattern == null) {
      _pattern = buildPattern(_value, _matchParameter);
    }
    return _pattern;
  }

  private Pattern buildPattern(String pattern, String matchParameter) {
    // Parse match parameters (Oracle-style)
    if (matchParameter != null) {
      for (char c : matchParameter.toCharArray()) {
        switch (c) {
          case 'i':
            return PatternFactory.compileCaseInsensitive(pattern);
          case 'c':
            return PatternFactory.compile(pattern);
          default:
            // Invalid character - default to case-sensitive
            return PatternFactory.compile(pattern);
        }
      }
    }

    // Default case-sensitive
    return PatternFactory.compile(pattern);
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
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value) && Objects.equals(_matchParameter,
        that._matchParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value, _matchParameter);
  }

  @Override
  public String toString() {
    if (_matchParameter != null && !_matchParameter.equals("c")) {
      return "regexp_like(" + _lhs + ",'" + _value + "','" + _matchParameter + "')";
    } else {
      return "regexp_like(" + _lhs + ",'" + _value + "')";
    }
  }
}
