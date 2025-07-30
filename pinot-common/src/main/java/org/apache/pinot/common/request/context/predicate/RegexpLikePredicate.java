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
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.common.utils.regex.PatternFactory;

/**
 * Predicate for REGEXP_LIKE with optional match parameters
 */
public class RegexpLikePredicate extends BasePredicate {
  private final String _value;
  private final boolean _caseInsensitive;
  private Pattern _pattern = null;

  public RegexpLikePredicate(ExpressionContext lhs, String value) {
    super(lhs);
    _value = value;
    _caseInsensitive = false;
  }

  public RegexpLikePredicate(ExpressionContext lhs, String value, String matchParameter) {
    super(lhs);
    _value = value;
    _caseInsensitive = RegexpPatternConverterUtils.isCaseInsensitive(matchParameter);
  }

  @Override
  public Type getType() {
    return Type.REGEXP_LIKE;
  }

  public String getValue() {
    return _value;
  }

  public boolean isCaseInsensitive() {
    return _caseInsensitive;
  }

  public Pattern getPattern() {
    if (_pattern == null) {
      _pattern = PatternFactory.compile(_value, _caseInsensitive);
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
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value) && Objects.equals(_caseInsensitive, that._caseInsensitive);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value, _caseInsensitive);
  }

  @Override
  public String toString() {
    if (_caseInsensitive) {
      return "regexp_like(" + _lhs + ",'" + _value + "','i')";
    } else {
      return "regexp_like(" + _lhs + ",'" + _value + "')";
    }
  }
}
