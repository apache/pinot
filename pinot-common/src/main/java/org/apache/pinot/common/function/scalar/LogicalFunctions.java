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
package org.apache.pinot.common.function.scalar;

import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Inbuilt logical transform functions with Trino-compatible NULL handling
 *
 * <p>These functions implement logical operators (AND, OR, NOT) with proper three-valued logic
 * support, following the SQL standard and Trino's behavior for NULL handling.
 *
 * <p>Truth tables for NULL handling:
 * <ul>
 *   <li>AND: Returns NULL if one side is NULL and the other is not FALSE.
 *       Returns FALSE if at least one side is FALSE.</li>
 *   <li>OR: Returns NULL if one side is NULL and the other is not TRUE.
 *       Returns TRUE if at least one side is TRUE.</li>
 *   <li>NOT: Returns NULL if the input is NULL.</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 * and(true, true) → true
 * and(true, false) → false
 * and(true, null) → null
 * and(false, null) → false
 *
 * or(true, false) → true
 * or(false, false) → false
 * or(false, null) → null
 * or(true, null) → true
 *
 * not(true) → false
 * not(false) → true
 * not(null) → null
 * </pre>
 */
public class LogicalFunctions {

  private LogicalFunctions() {
  }

  /**
   * Logical AND operator with Trino-compatible NULL handling.
   *
   * <p>Truth table:
   * <pre>
   * a     | b     | result
   * ------|-------|-------
   * TRUE  | TRUE  | TRUE
   * TRUE  | FALSE | FALSE
   * TRUE  | NULL  | NULL
   * FALSE | TRUE  | FALSE
   * FALSE | FALSE | FALSE
   * FALSE | NULL  | FALSE
   * NULL  | TRUE  | NULL
   * NULL  | FALSE | FALSE
   * NULL  | NULL  | NULL
   * </pre>
   *
   * @param a First boolean value (nullable)
   * @param b Second boolean value (nullable)
   * @return The logical AND result, or NULL if result cannot be determined
   */
  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Boolean and(@Nullable Boolean a, @Nullable Boolean b) {
    // If either value is FALSE, the result is FALSE
    if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
      return false;
    }

    // If both values are TRUE, the result is TRUE
    if (Boolean.TRUE.equals(a) && Boolean.TRUE.equals(b)) {
      return true;
    }

    // Otherwise, at least one value is NULL and neither is FALSE, so result is NULL
    return null;
  }

  /**
   * Logical OR operator with Trino-compatible NULL handling.
   *
   * <p>Truth table:
   * <pre>
   * a     | b     | result
   * ------|-------|-------
   * TRUE  | TRUE  | TRUE
   * TRUE  | FALSE | TRUE
   * TRUE  | NULL  | TRUE
   * FALSE | TRUE  | TRUE
   * FALSE | FALSE | FALSE
   * FALSE | NULL  | NULL
   * NULL  | TRUE  | TRUE
   * NULL  | FALSE | NULL
   * NULL  | NULL  | NULL
   * </pre>
   *
   * @param a First boolean value (nullable)
   * @param b Second boolean value (nullable)
   * @return The logical OR result, or NULL if the result cannot be determined
   */
  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Boolean or(@Nullable Boolean a, @Nullable Boolean b) {
    // If either value is TRUE, the result is TRUE
    if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
      return true;
    }

    // If both values are FALSE, the result is FALSE
    if (Boolean.FALSE.equals(a) && Boolean.FALSE.equals(b)) {
      return false;
    }

    // Otherwise, at least one value is NULL and neither is TRUE, so result is NULL
    return null;
  }

  /**
   * Logical NOT operator with Trino-compatible NULL handling.
   *
   * <p>Truth table:
   * <pre>
   * a     | result
   * ------|-------
   * TRUE  | FALSE
   * FALSE | TRUE
   * NULL  | NULL
   * </pre>
   *
   * @param a Boolean value to negate (nullable)
   * @return The logical negation of the input, or NULL if the input is NULL
   */
  @Nullable
  @ScalarFunction(nullableParameters = true)
  public static Boolean not(@Nullable Boolean a) {
    if (a == null) {
      return null;
    }
    return !a;
  }
}
