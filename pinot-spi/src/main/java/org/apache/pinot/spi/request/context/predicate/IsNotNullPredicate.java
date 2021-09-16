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
package org.apache.pinot.spi.request.context.predicate;

import java.util.Objects;
import org.apache.pinot.spi.request.context.ExpressionContext;


/**
 * Predicate for IS_NOT_NULL.
 */
public class IsNotNullPredicate implements Predicate {
  private final ExpressionContext _lhs;

  public IsNotNullPredicate(ExpressionContext lhs) {
    _lhs = lhs;
  }

  @Override
  public Type getType() {
    return Type.IS_NOT_NULL;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IsNotNullPredicate)) {
      return false;
    }
    IsNotNullPredicate that = (IsNotNullPredicate) o;
    return Objects.equals(_lhs, that._lhs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs);
  }

  @Override
  public String toString() {
    return _lhs + " IS NOT NULL";
  }
}
