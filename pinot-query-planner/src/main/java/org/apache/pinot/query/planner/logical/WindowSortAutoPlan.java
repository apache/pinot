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
package org.apache.pinot.query.planner.logical;


/// Selects ordering for global windows on the finalized logical tree, before stage allocation.
/// Implementations must fail closed when evidence is absent or ambiguous. The logical key remains stable across
/// repeated compilations; allocated stage identifiers are bound separately for one query's observations.
public interface WindowSortAutoPlan {
  boolean useSenderSort(ExchangeKey key);

  default boolean shouldProfile(ExchangeKey key) {
    return true;
  }

  /// Binds an already selected logical exchange to this query's receiver stage. It cannot change the decision.
  default void bind(ExchangeKey key, int receiverStageId, int senderStageId) {
  }

  /// A deterministic path in the finalized logical tree plus ordering/input fingerprints, without stage identifiers.
  record ExchangeKey(String path, int inputHash, int collationHash) {
  }
}
