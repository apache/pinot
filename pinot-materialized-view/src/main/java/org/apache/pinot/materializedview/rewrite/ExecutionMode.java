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
package org.apache.pinot.materializedview.rewrite;


/// Determines how the broker should execute an MV-rewritten query.
public enum ExecutionMode {

  /// The MV fully replaces the base table — the rewritten query targets the MV
  /// table and no base-table query is needed.
  FULL_REWRITE,

  /// The MV covers only historical data (ts &lt; watermarkMs). The broker
  /// issues two queries — one against the MV and one against the base table
  /// (ts &gt;= watermarkMs) — and merges the results.
  SPLIT_REWRITE
}
