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

/// **Definition — MV SQL validation.**
///
/// Parses and validates the `definedSQL` field on the MV's `TableConfig` at create time:
/// legal aggregation functions, group-by / dimension column coverage, supported time-bucket
/// expressions.  Runs in the controller before the MV znode is persisted, so a malformed
/// definition is rejected with a clear error rather than producing broken segments later.
///
/// See also [org.apache.pinot.materializedview.metadata] for the persisted definition
/// schema and [org.apache.pinot.materializedview.scheduler] for the consumer that turns a
/// validated definition into minion tasks.
package org.apache.pinot.materializedview.analysis;
