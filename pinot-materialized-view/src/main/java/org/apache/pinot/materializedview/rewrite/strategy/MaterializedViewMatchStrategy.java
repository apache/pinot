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
package org.apache.pinot.materializedview.rewrite.strategy;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;


/// Strategy interface for matching a user query against a candidate materialized view.
///
/// Implementations produce an [MaterializedViewRewritePlan] fragment containing the
/// rewritten MV query, match type, and cost. Execution mode and base query
/// template are resolved later by
/// [MaterializedViewQueryRewriteEngine][org.apache.pinot.materializedview.rewrite.MaterializedViewQueryRewriteEngine].
///
/// Implementations should be stateless and thread-safe. New matching algorithms
/// (partial aggregation, column projection, time-range rollup, etc.) can be added
/// by implementing this interface and registering with `MaterializedViewQueryRewriteEngine`.
public interface MaterializedViewMatchStrategy {

  /// Attempts to match the given user query against the candidate MV entry.
  ///
  /// @param userQuery      the compiled PinotQuery from the user's SQL
  /// @param candidateEntry the cached MV entry (metadata + pre-compiled definedSql query)
  /// @return an [MaterializedViewRewritePlan] fragment if the MV can answer the user query,
  ///         `null` otherwise
  @Nullable
  MaterializedViewRewritePlan match(PinotQuery userQuery, MaterializedViewCacheEntry candidateEntry);
}
