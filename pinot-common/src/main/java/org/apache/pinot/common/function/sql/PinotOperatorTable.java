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
package org.apache.pinot.common.function.sql;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.pinot.common.function.FunctionRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Temporary implementation of all dynamic arg/return type inference operators.
 * TODO: merge this with {@link PinotCalciteCatalogReader} once we support
 *     1. Return/Inference configuration in @ScalarFunction
 *     2. Allow @ScalarFunction registry towards class (with multiple impl)
 */
public class PinotOperatorTable implements SqlOperatorTable {
  private static final PinotOperatorTable INSTANCE = new PinotOperatorTable();

  public static synchronized PinotOperatorTable instance() {
    return INSTANCE;
  }

  @Override public void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    String simpleName = opName.getSimple();
    final Collection<SqlOperator> list =
        lookUpOperators(simpleName);
    if (list.isEmpty()) {
      return;
    }
    for (SqlOperator op : list) {
      if (op.getSyntax() == syntax) {
        operatorList.add(op);
      } else if (syntax == SqlSyntax.FUNCTION
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        operatorList.add(op);
      }
    }

    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    switch (syntax) {
      case BINARY:
      case PREFIX:
      case POSTFIX:
        for (SqlOperator extra
            : lookUpOperators(simpleName)) {
          // REVIEW: should only search operators added during this method?
          if (extra != null && !operatorList.contains(extra)) {
            operatorList.add(extra);
          }
        }
        break;
      default:
        break;
    }
  }

  /**
   * Look up operators based on case-sensitiveness.
   */
  private Collection<SqlOperator> lookUpOperators(String name) {
    return PinotFunctionRegistry.getOperatorMap().range(name, FunctionRegistry.CASE_SENSITIVITY).stream()
        .map(Map.Entry::getValue).collect(Collectors.toSet());
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return PinotFunctionRegistry.getOperatorMap().map().values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
