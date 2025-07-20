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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Predicate for TEXT_MATCH.
 */
public class TextMatchPredicate extends BasePredicate {
  private final String _value;
  private final String _options;

  public TextMatchPredicate(ExpressionContext lhs, String value) {
    this(lhs, value, null);
  }

  public TextMatchPredicate(ExpressionContext lhs, String value, @Nullable String options) {
    super(lhs);
    _value = value;
    _options = options;
  }

  @Override
  public Type getType() {
    return Type.TEXT_MATCH;
  }

  public String getValue() {
    return _value;
  }

  @Nullable
  public String getOptions() {
    return _options;
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
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value) && Objects.equals(_options, that._options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value, _options);
  }

  @Override
  public String toString() {
    if (_options != null) {
      return "text_match(" + _lhs + ",'" + _value + "','" + _options + "')";
    } else {
      return "text_match(" + _lhs + ",'" + _value + "')";
    }
  }
}
