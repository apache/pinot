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

/// **Query rewrite — broker SPI surface.**
///
/// The broker delegates every MV concern through
/// [MaterializedViewHandler][org.apache.pinot.materializedview.handler.MaterializedViewHandler]
/// so the bulk of MV-aware logic stays out of `pinot-broker`.  Hooks:
///
///   - [#compile][org.apache.pinot.materializedview.handler.MaterializedViewHandler#compile] —
///     match the user query against the MV catalog; return a
///     [MaterializedViewContext][org.apache.pinot.materializedview.context.MaterializedViewContext]
///     describing the rewrite decision (no rewrite / full rewrite / split rewrite).
///   - [#executeSplit][org.apache.pinot.materializedview.handler.MaterializedViewHandler#executeSplit] —
///     own the dual scatter-gather + merge flow when the compile decision was SPLIT_REWRITE.
///     The broker supplies the dispatcher callback that performs the actual server I/O.
///   - [#annotateResponse][org.apache.pinot.materializedview.handler.MaterializedViewHandler#annotateResponse] —
///     stamp the MV-used metadata onto the broker response.
///   - [#invalidateBaseTable][org.apache.pinot.materializedview.handler.MaterializedViewHandler#invalidateBaseTable]
///     / [#refreshTable][org.apache.pinot.materializedview.handler.MaterializedViewHandler#refreshTable] —
///     called from the broker resource state model when a base table or MV table cycles
///     ONLINE↔OFFLINE/DROPPED so the handler's cache stays consistent.
///   - [#close][org.apache.pinot.materializedview.handler.MaterializedViewHandler#close] —
///     release ZK listener slots / executor pools on broker shutdown.
///
/// [DefaultMaterializedViewHandler][org.apache.pinot.materializedview.handler.DefaultMaterializedViewHandler]
/// is the in-tree implementation; operators can wire a custom handler via the
/// `pinot.broker.materialized.view.handler.class` config.
package org.apache.pinot.materializedview.handler;
