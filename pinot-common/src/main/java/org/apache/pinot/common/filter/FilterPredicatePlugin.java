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
package org.apache.pinot.common.filter;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;


/**
 * SPI interface for registering custom filter predicates into Pinot.
 *
 * <p>A plugin implementing this interface can introduce a new filter predicate (e.g., SEMANTIC_MATCH)
 * without modifying Pinot core code. The plugin handles three layers:
 * <ol>
 *   <li><b>SQL Parsing</b> — validation and rewriting of the Thrift expression tree</li>
 *   <li><b>Calcite Registration</b> — operand type metadata for the SQL operator</li>
 *   <li><b>Predicate Creation</b> — converting parsed operands into a {@link Predicate} object</li>
 * </ol>
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader} or registered programmatically
 * through {@link FilterPredicateRegistry#register(FilterPredicatePlugin)}.
 *
 * <p>Thread-safety: implementations must be stateless and thread-safe.
 */
public interface FilterPredicatePlugin {

  /**
   * Returns the canonical name of this filter predicate (e.g., "SEMANTIC_MATCH").
   * Must be unique across all registered plugins and built-in filter kinds.
   * The name is case-insensitive and will be uppercased for matching.
   */
  String name();

  // --- SQL Parsing Layer (CalciteSqlParser) ---

  /**
   * Validates the filter expression operands during SQL parsing.
   * Called from {@code CalciteSqlParser.validateFilter()}.
   *
   * @param operands the operand expressions from the Thrift function call
   * @throws IllegalStateException if the operands are invalid
   */
  void validateFilterExpression(List<org.apache.pinot.common.request.Expression> operands);

  /**
   * Rewrites the predicate expression during query rewriting.
   * Called from {@code PredicateComparisonRewriter.updateFunctionExpression()}.
   *
   * <p>The default implementation performs basic validation that all operands except the first
   * are literals. Override to customize validation or rewriting logic.
   *
   * @param operands the operand expressions from the Thrift function call
   * @throws org.apache.pinot.spi.exception.SqlCompilationException if the expression is invalid
   */
  default void rewriteExpression(List<org.apache.pinot.common.request.Expression> operands) {
    // Default: no-op, pass through as-is
  }

  // --- Calcite Operator Registration (PinotOperatorTable) ---

  /**
   * Returns the operand type families for Calcite SQL operator registration.
   * Each element corresponds to one operand and specifies its expected SQL type family.
   *
   * <p>Example for SEMANTIC_MATCH(column, 'query', topK):
   * {@code List.of(OperandType.STRING, OperandType.STRING, OperandType.INTEGER)}
   */
  List<OperandType> getOperandTypes();

  /**
   * Returns the indices of optional operands (0-based).
   * These operands may be omitted when calling the function.
   *
   * <p>Example: if the third operand (index 2) is optional, return {@code List.of(2)}.
   */
  default List<Integer> getOptionalOperandIndices() {
    return List.of();
  }

  // --- Predicate Creation Layer (RequestContextUtils) ---

  /**
   * Creates a {@link Predicate} from the parsed operands.
   * Called from {@code RequestContextUtils.getFilterInner()} when the filter name matches this plugin.
   *
   * @param operands the operand expressions as ExpressionContext objects
   * @return the predicate object to use for filter evaluation
   */
  Predicate createPredicate(List<ExpressionContext> operands);

  /**
   * Supported operand types for Calcite SQL operator registration.
   * These map to Calcite's {@code SqlTypeFamily} values without requiring a Calcite dependency.
   */
  enum OperandType {
    STRING,
    INTEGER,
    NUMERIC,
    ARRAY,
    BOOLEAN,
    ANY
  }
}
