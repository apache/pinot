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
package org.apache.pinot.common.function;

import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


/// The SQL functions that read rows from somewhere other than the FROM clause of the query that calls them, which the
/// access checks keyed on that FROM clause do not cover: table-level authorization, row-level security and the DELETE
/// predicate rules each decide how to treat every one of them.
///
/// Nothing derives this list from the function registries, so a function that reads another table or runs a nested
/// query must be added here when it is defined. The single-stage broker then rejects it for a principal with row
/// filters on the queried table, and DELETE rejects it in its WHERE clause, with no further decision.
///
/// [#matches(String)] is the one spelling check every caller shares: underscores dropped and case folded per
/// character, as [String#equalsIgnoreCase(String)] does, so that the broker's checks and the server's resolver agree on
/// every spelling, including a look-alike such as a dotless `ı` (U+0131) for the `i`, which a `toLowerCase()`
/// comparison leaves apart, and a name lower-cased under a Turkish locale.
public enum CrossTableFunction {
  /// `lookup('dimTable', 'valueColumn', 'joinKey', key, ...)`: joins one row of a dimension table by primary key on
  /// the server, from the table's in-memory data and never through a filter. `TransformFunctionType#LOOKUP`, with
  /// `LookupTransformFunction` in pinot-core.
  LOOKUP,
  /// `IN_SUBQUERY(expr, 'sql')`: the broker runs the quoted query as a request of its own and rewrites the call to
  /// `IN_ID_SET` on the result, see `BaseSingleStageBrokerRequestHandler` and `IdSetSubqueryRewriter` in
  /// pinot-broker.
  IN_SUBQUERY,
  /// `IN_PARTITIONED_SUBQUERY(expr, 'sql')`: the server runs the quoted query on the segments of the queried table it
  /// holds and rewrites the call to `IN_ID_SET` on the result, see `ServerQueryExecutorV1Impl` in pinot-core.
  IN_PARTITIONED_SUBQUERY;

  private static final CrossTableFunction[] VALUES = values();

  private final String _nameWithoutUnderscores = StringUtils.remove(name(), '_');

  /// Returns whether `functionName`, as written in a query or as the parser canonicalizes it, names this function.
  /// `null`, which a parse tree node that is not a function call has for a name, names none.
  public boolean matches(@Nullable String functionName) {
    return functionName != null && StringUtils.remove(functionName, '_').equalsIgnoreCase(_nameWithoutUnderscores);
  }

  /// Returns the function that `functionName` names, or `null` if it names none of them.
  @Nullable
  public static CrossTableFunction fromFunctionName(@Nullable String functionName) {
    if (functionName == null) {
      return null;
    }
    for (CrossTableFunction function : VALUES) {
      if (function.matches(functionName)) {
        return function;
      }
    }
    return null;
  }
}
