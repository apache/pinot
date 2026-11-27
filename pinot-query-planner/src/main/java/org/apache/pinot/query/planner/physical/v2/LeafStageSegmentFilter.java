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
package org.apache.pinot.query.planner.physical.v2;

import java.util.Map;


/// Extension point that lets the V2 physical optimizer narrow the segments that a physical leaf-stage table scan reads,
/// once the routing table has been turned into a per-worker assignment. It is the V2 counterpart of the
/// `WorkerManager` hook that the V1 worker assignment calls for the same purpose.
///
/// The filter runs for scans of physical tables only; scans of logical tables are not passed to it.
///
/// An implementation must keep every worker id of the metadata it is given, because the data distribution of the scan
/// was already derived from those workers. A worker whose segments are all filtered out stays in the assignment with
/// empty segment lists. Segments can be removed only by replacing the per-worker segment lists (or the whole map): the
/// lists and maps that are handed over can be shared with the routing table, and the inner maps can be immutable, so
/// they must not be modified in place. The unavailable segments and the time boundary are not filtered by the caller.
public interface LeafStageSegmentFilter {

  /// Returns the metadata to use for the scan, which can be the given instance when nothing is filtered.
  ///
  /// @param metadata the physical table scan metadata built from the routing table
  /// @param queryOptions the query options of the request
  TableScanMetadata filterLeafStageSegments(TableScanMetadata metadata, Map<String, String> queryOptions);
}
