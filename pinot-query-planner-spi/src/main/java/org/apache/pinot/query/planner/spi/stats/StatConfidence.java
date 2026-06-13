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
package org.apache.pinot.query.planner.spi.stats;


/// Indicates how trustworthy a statistics value is for cost-based query planning.
///
/// Callers should treat [#LOW] statistics the same as [#UNKNOWN] for
/// cost-based decisions because LOW values are known to be systematically biased.
///
/// This enum is append-only — new confidence levels may be added without breaking
/// code compiled against an older version. Existing constants must never be reordered
/// or removed.
///
/// Thread-safety: enum constants are inherently thread-safe.
public enum StatConfidence {
  /// Derived from authoritative metadata (e.g. sum of committed segments' totalDocs for an
  /// OFFLINE table).
  EXACT,

  /// Derived via approximation (e.g. clamped NDV merge, interpolated time ranges).
  ESTIMATED,

  /// Known to be systematically biased (e.g. upsert tables where physical doc count over-counts
  /// logical rows; tables with consuming segments). Planner must treat LOW like absent stats for
  /// cost-based decisions.
  LOW,

  /// No information available.
  UNKNOWN
}
