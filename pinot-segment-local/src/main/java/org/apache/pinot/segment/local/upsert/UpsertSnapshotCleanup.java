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
import javax.annotation.Nullable;


/// Observes actual cleanup passes, without per-key work or synchronization with snapshotting. A target becomes a
/// completed watermark only after all effects and dirty bookkeeping succeed. Producers serialize their own passes;
/// an unexpected overlapping observation is conservatively treated as possibly partial.
public final class UpsertSnapshotCleanup {
  private final AtomicReference<UpsertSnapshotMetadata.CleanupProgress> _state;

  public UpsertSnapshotCleanup(boolean enabled) {
    _state = new AtomicReference<>(enabled
        ? new UpsertSnapshotMetadata.CleanupProgress(0, "NOT_RUN", null, null, true, 0)
        : UpsertSnapshotMetadata.CleanupProgress.disabled());
  }

  public UpsertSnapshotMetadata.CleanupProgress read() {
    return _state.get();
  }

  public UpsertSnapshotMetadata.CleanupProgress begin(@Nullable Double targetWatermark, boolean mayAffectValidDocIds) {
    Double target = targetWatermark != null && Double.isFinite(targetWatermark) ? targetWatermark : null;
    return _state.updateAndGet(previous -> new UpsertSnapshotMetadata.CleanupProgress(previous.version() + 1,
        "RUNNING", previous.lastCompletedWatermark(), target, mayAffectValidDocIds, previous.failedPasses()));
  }

  public void end(UpsertSnapshotMetadata.CleanupProgress started, boolean succeeded) {
    _state.updateAndGet(previous -> {
      boolean completed = succeeded && previous == started;
      Double watermark = completed && started.targetWatermark() != null ? started.targetWatermark()
          : previous.lastCompletedWatermark();
      return new UpsertSnapshotMetadata.CleanupProgress(previous.version() + 1,
          completed ? "COMPLETED" : "FAILED_POSSIBLY_PARTIAL", watermark, started.targetWatermark(),
          started.mayAffectValidDocIds(), previous.failedPasses() + (completed ? 0 : 1));
    });
  }
}
