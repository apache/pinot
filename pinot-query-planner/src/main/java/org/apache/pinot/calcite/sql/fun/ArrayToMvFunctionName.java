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
package org.apache.pinot.calcite.sql.fun;

/// The name of the `ARRAY_TO_MV` function and the one check that recognizes it. Every planner layer (Calcite
/// rules, the leaf-stage query translation and the plan-node validation) uses [#matches(String)], so that they all
/// agree on which spellings name the function.
///
/// The class has no state and no dependencies, so it is safe to load on servers and is thread-safe.
public final class ArrayToMvFunctionName {
  /// The name under which `PinotOperatorTable` registers the function.
  public static final String NAME = "ARRAY_TO_MV";
  /// The canonical name: lower case, without underscores.
  private static final String CANONICAL_NAME = "arraytomv";

  private ArrayToMvFunctionName() {
  }

  /// Returns whether `functionName` names `ARRAY_TO_MV`, ignoring case and underscores: `ARRAY_TO_MV`, `arrayToMv`
  /// and `ARRAYTOMV` all match.
  public static boolean matches(String functionName) {
    return functionName.replace("_", "").equalsIgnoreCase(CANONICAL_NAME);
  }
}
