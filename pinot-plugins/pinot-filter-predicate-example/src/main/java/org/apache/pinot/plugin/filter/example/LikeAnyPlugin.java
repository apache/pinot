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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.filter.FilterPredicatePlugin;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;


/**
 * Plugin that registers the LIKE_ANY filter predicate into Pinot.
 *
 * <p>Usage: {@code SELECT * FROM myTable WHERE LIKE_ANY(col, 'pattern1', 'pattern2', ...)}
 *
 * <p>Matches rows where the column value matches ANY of the LIKE patterns (OR semantics).
 * Each pattern uses standard SQL LIKE syntax: {@code %} matches any sequence, {@code _} matches one character.
 */
public class LikeAnyPlugin implements FilterPredicatePlugin {

  @Override
  public String name() {
    return "LIKE_ANY";
  }

  @Override
  public void validateFilterExpression(List<Expression> operands) {
    if (operands.size() < 2) {
      throw new IllegalStateException("LIKE_ANY requires at least 2 arguments: LIKE_ANY(column, pattern1, ...)");
    }
    if (!operands.get(0).isSetIdentifier()) {
      throw new IllegalStateException("The first argument of LIKE_ANY must be a column identifier");
    }
    for (int i = 1; i < operands.size(); i++) {
      if (!operands.get(i).isSetLiteral() || operands.get(i).getLiteral().isSetNullValue()) {
        throw new IllegalStateException(
            "Arguments 2+ of LIKE_ANY must be non-null string literals (LIKE patterns)");
      }
    }
  }

  @Override
  public List<OperandType> getOperandTypes() {
    // column + at least one pattern; Calcite sees variadic as (STRING, STRING)
    return List.of(OperandType.STRING, OperandType.STRING);
  }

  @Override
  public List<Integer> getOptionalOperandIndices() {
    // Second operand is required but additional patterns are variadic — handled at runtime
    return List.of();
  }

  @Override
  public Predicate createPredicate(List<ExpressionContext> operands) {
    ExpressionContext column = operands.get(0);
    List<String> patterns = new ArrayList<>(operands.size() - 1);
    for (int i = 1; i < operands.size(); i++) {
      patterns.add(operands.get(i).getLiteral().getStringValue());
    }
    return new LikeAnyPredicate(column, patterns);
  }
}
