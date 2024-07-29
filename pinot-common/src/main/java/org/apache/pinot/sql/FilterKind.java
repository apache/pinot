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
package org.apache.pinot.sql;

public enum FilterKind {
  AND,
  OR,
  NOT,
  EQUALS,
  NOT_EQUALS,
  GREATER_THAN,
  GREATER_THAN_OR_EQUAL,
  LESS_THAN,
  LESS_THAN_OR_EQUAL,
  BETWEEN,
  RANGE,
  IN,
  NOT_IN,
  LIKE,
  REGEXP_LIKE,
  TEXT_CONTAINS,
  TEXT_MATCH,
  JSON_MATCH,
  IS_NULL,
  IS_NOT_NULL,
  VECTOR_SIMILARITY;

  /**
   * Helper method that returns true if the enum maps to a Range.
   *
   * @return True if the enum is of Range type, false otherwise.
   */
  public boolean isRange() {
    return this == GREATER_THAN || this == GREATER_THAN_OR_EQUAL || this == LESS_THAN || this == LESS_THAN_OR_EQUAL
        || this == BETWEEN || this == RANGE;
  }
}
