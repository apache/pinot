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
package org.apache.pinot.pql.parsers.pql2.ast;

public enum FilterKind {
  AND(false, false),
  OR(false, false),
  NOT(false, false),
  EQUALS(true, false),
  NOT_EQUALS(true, false),
  GREATER_THAN(true, true),
  GREATER_THAN_OR_EQUAL(true, true),
  LESS_THAN(true, true),
  LESS_THAN_OR_EQUAL(true, true),
  LIKE(true, false),
  BETWEEN(true, true),
  RANGE(true, true),
  IN(true, false),
  NOT_IN(true, false),
  REGEXP_LIKE(true, false),
  IS_NULL(true, false),
  IS_NOT_NULL(true, false),
  TEXT_MATCH(true, false),
  JSON_MATCH(true, false);

  private final boolean _isPredicate;
  private final boolean _isRange;

  FilterKind(boolean isPredicate, boolean isRange) {
    _isPredicate = isPredicate;
    _isRange = isRange;
  }

  /** @return true if the enum is of Range type, false otherwise. */
  public boolean isRange() {
    return _isRange;
  }

  /** @return true if the filter is a predicate. This logic should mimic FilterContext.Type. */
  public boolean isPredicate() {
    return _isPredicate;
  }
}
