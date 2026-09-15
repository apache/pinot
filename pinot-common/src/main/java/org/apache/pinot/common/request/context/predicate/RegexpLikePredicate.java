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

/// Predicate for `REGEXP_LIKE` with optional match parameters.
///
/// Instances are read concurrently: a predicate belongs to the query's filter tree, which is built once per query and
/// then shared by the threads that build and run the per-segment plans. The lazily compiled pattern is therefore
/// published safely; see [#getPattern].
public class RegexpLikePredicate extends BasePredicate {
  private final String _value;
  private final boolean _caseInsensitive;

  /// Lazily compiled cache of [#getPattern].
  ///
  /// `volatile` is required, not just for the null check in [#getPattern]: without it the pattern is published
  /// unsafely, and a racing thread can read the non-null reference while the fields written by the pattern's
  /// construction are still invisible to it. Using such a pattern to create a matcher fails with a
  /// `NullPointerException`.
  private volatile Pattern _pattern;

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

  /// Returns the compiled pattern, lazily compiling and caching it on first access.
  ///
  /// Uses the racy-single-check idiom: two threads may each compile the pattern, but both compile the same one, so
  /// the duplicate work is harmless. Correctness relies on `_pattern` being `volatile`; see the field for why.
  public Pattern getPattern() {
    Pattern pattern = _pattern;
    if (pattern == null) {
      pattern = PatternFactory.compile(_value, _caseInsensitive);
      _pattern = pattern;
    }
    return pattern;
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
