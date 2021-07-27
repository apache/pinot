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

import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * The {@code Predicate} class represents the predicate in the filter.
 * <p>Currently the query engine only accepts string literals as the right-hand side of the predicate, so we store the
 * right-hand side of the predicate as string or list of strings.
 */
public interface Predicate {
  enum Type {
    EQ, NOT_EQ, IN, NOT_IN, RANGE, REGEXP_LIKE, LIKE, TEXT_MATCH, JSON_MATCH, IS_NULL, IS_NOT_NULL;

    public boolean isExclusive() {
      return this == NOT_EQ || this == NOT_IN || this == IS_NOT_NULL;
    }
  }

  /**
   * Returns the type of the predicate.
   */
  Type getType();

  /**
   * Returns the left-hand side expression of the predicate.
   */
  ExpressionContext getLhs();
}
