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

/// **Definition — ZK znode schemas.**
///
/// The materialized-view subsystem persists two znodes per MV table:
///
///   - **Definition znode** — `MaterializedViewDefinitionMetadata`.  Slowly-changing:
///     `definedSQL`, base table list, rewrite-enable flag, optional split spec.  Written by
///     the controller at create time; read by the broker's MV metadata cache.
///   - **Runtime znode** — `MaterializedViewRuntimeMetadata`.  Rapidly-changing: watermark,
///     per-partition state.  Written by the minion task on each successful refresh; read by
///     the broker's MV metadata cache.
///
/// Both znodes live under `PROPERTYSTORE/MATERIALIZED_VIEW/{DEFINITION,RUNTIME}` paths,
/// the broker's cache subscribes to child-change + data-change events on both parent
/// paths so updates propagate to the rewrite engine without polling.
package org.apache.pinot.materializedview.metadata;
