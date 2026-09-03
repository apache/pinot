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
package org.apache.pinot.query.runtime.operator;

import org.apache.pinot.query.planner.plannode.ExplainedNode;


/// A leaf-stage operator that can describe itself as an [ExplainedNode] for {@code EXPLAIN}.
///
/// {@code QueryRunner#explainQuery} compiles the leaf stage with {@code explain=true} and, for each
/// operator built, records the [ExplainedNode] returned here so it can be spliced back into the
/// broker plan tree. The default row leaf ([LeafOperator]) implements this, and alternative leaf-stage
/// operators can implement it too so their execution shows up in {@code EXPLAIN} instead of falling
/// back to the pre-execution Calcite plan.
public interface ExplainableOperator {

  /// Produces the explain representation of this operator's stage subtree. May run the single-stage
  /// engine in explain mode to obtain segment-level plans; it does not execute the multi-stage query.
  ExplainedNode explain();
}
