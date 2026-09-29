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

/// **Query rewrite — compile-time carrier.**
///
/// [MaterializedViewContext][org.apache.pinot.materializedview.context.MaterializedViewContext]
/// is the immutable handoff object the broker's
/// [MaterializedViewHandler][org.apache.pinot.materializedview.handler.MaterializedViewHandler]
/// returns from `compile()`.  It tells the broker which mode applies:
///
///   - `isFullRewrite()` — swap the server query to target the MV; route + scatter-gather
///     run against the MV table.
///   - `isSplitRewrite()` — dispatch dual scatter-gather (base + MV) and merge results.
///   - neither — base-table path unchanged.
///
/// For SPLIT mode the context also carries the MV-side query, table name, and schema that
/// the split dispatcher needs to build the second scatter-gather; the broker keeps its own
/// pre-rewrite locals for the base side.
package org.apache.pinot.materializedview.context;
