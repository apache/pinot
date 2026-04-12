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
package org.apache.pinot.plugin.filter.example;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.CustomPredicate;


/**
 * Predicate for LIKE_ANY(column, 'pattern1', 'pattern2', ...).
 * Matches rows where the column value matches ANY of the given LIKE patterns.
 *
 * <p>Example SQL: {@code SELECT * FROM myTable WHERE LIKE_ANY(name, 'John%', '%Smith', 'A_e')}
 */
public class LikeAnyPredicate extends CustomPredicate {

  private final List<String> _patterns;

  public LikeAnyPredicate(ExpressionContext lhs, List<String> patterns) {
    super(lhs, "LIKE_ANY");
    _patterns = patterns;
  }

  public List<String> getPatterns() {
    return _patterns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LikeAnyPredicate)) {
      return false;
    }
    LikeAnyPredicate that = (LikeAnyPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_patterns, that._patterns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _patterns);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("like_any(").append(_lhs);
    for (String pattern : _patterns) {
      builder.append(", '").append(pattern).append('\'');
    }
    return builder.append(')').toString();
  }
}
