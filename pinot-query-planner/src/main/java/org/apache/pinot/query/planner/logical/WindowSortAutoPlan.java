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


/// Selects one ordering strategy for every worker of a global window exchange.
///
/// The receiver and sender stage identifiers are stable within one planned query. Implementations must return false
/// when evidence for this exchange is absent or ambiguous. A selector is shared only during planning and must not
/// mutate the physical plan after dispatch.
public interface WindowSortAutoPlan {
  boolean useSenderSort(int receiverStageId, int senderStageId, int inputHash, int collationHash);

  /// Whether a receiver-sort execution should collect a fresh AUTO sample. The default preserves existing selectors.
  default boolean shouldProfile(int receiverStageId, int senderStageId, int inputHash, int collationHash) {
    return true;
  }
}
