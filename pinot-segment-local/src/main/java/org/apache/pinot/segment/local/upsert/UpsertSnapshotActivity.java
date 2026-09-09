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
package org.apache.pinot.segment.local.upsert;

import java.util.concurrent.atomic.AtomicReference;


/// Observes coarse operations without acquiring their locks or changing their execution order. Start/end both
/// advance the epoch, so a complete operation between two observations is visible. No ingestion-row instrumentation.
public final class UpsertSnapshotActivity {
  private final AtomicReference<UpsertSnapshotMetadata.Activity> _state =
      new AtomicReference<>(new UpsertSnapshotMetadata.Activity(0, 0, 0));

  public UpsertSnapshotMetadata.Activity read() {
    return _state.get();
  }

  public UpsertSnapshotMetadata.Activity begin() {
    return _state.updateAndGet(state -> new UpsertSnapshotMetadata.Activity(state.version() + 1,
        state.activeOperations() + 1, state.failedOperations()));
  }

  public UpsertSnapshotMetadata.Activity end(boolean succeeded) {
    return _state.updateAndGet(state -> new UpsertSnapshotMetadata.Activity(state.version() + 1,
        state.activeOperations() - 1, state.failedOperations() + (succeeded ? 0 : 1)));
  }
}
