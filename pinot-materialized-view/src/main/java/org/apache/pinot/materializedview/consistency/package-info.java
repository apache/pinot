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

/// **Definition — controller-side reconciler.**
///
/// Cross-checks materialized-view state across the base-table TableConfig, the MV's own
/// TableConfig, the MV definition znode in [org.apache.pinot.materializedview.metadata],
/// and any in-flight minion tasks.  Resolves drift caused by base-table drops, renames,
/// ingestion-config changes, or partial controller failure during MV creation.
///
/// Runs on the controller leader; the broker and minion are unaware of this package.
package org.apache.pinot.materializedview.consistency;
