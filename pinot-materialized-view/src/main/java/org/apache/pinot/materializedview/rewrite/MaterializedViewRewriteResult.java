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

import java.util.List;
import javax.annotation.Nullable;


/// Wraps the outcome of a materialized-view rewrite attempt, carrying both the
/// full list of candidate MV table names that were evaluated and the optional
/// best rewrite plan.
///
/// An instance is always created when candidate MVs exist for the queried
/// base table, regardless of whether any MV actually matched. This allows the
/// broker response to report observability information (candidate list and hit
/// status) even on a miss.
///
/// This class is immutable and thread-safe.
public class MaterializedViewRewriteResult {
  private final List<String> _candidateNames;
  @Nullable
  private final MaterializedViewRewritePlan _plan;

  public MaterializedViewRewriteResult(List<String> candidateNames, @Nullable MaterializedViewRewritePlan plan) {
    _candidateNames = List.copyOf(candidateNames);
    _plan = plan;
  }

  public List<String> getCandidateNames() {
    return _candidateNames;
  }

  @Nullable
  public MaterializedViewRewritePlan getPlan() {
    return _plan;
  }

  public boolean isHit() {
    return _plan != null;
  }

  @Nullable
  public String getMaterializedViewQueriedName() {
    return _plan != null ? _plan.getMaterializedViewTableNameWithType() : null;
  }
}
