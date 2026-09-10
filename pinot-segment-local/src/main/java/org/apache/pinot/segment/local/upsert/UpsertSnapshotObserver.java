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

import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;


/// Observes one existing snapshot attempt on its calling thread. Different attempts may run concurrently.
/// Each visited immutable segment is first SELECTED or UNCHANGED. Selected segments then receive an outcome,
/// unless the attempt aborts before reaching them. Runtime exceptions from callbacks are logged and ignored.
/// Implementations must not mutate segments or retain bitmap bytes; the snapshot algorithm owns those objects.
public interface UpsertSnapshotObserver {
  enum Outcome {
    SELECTED, UNCHANGED, WRITTEN, LOCK_SKIPPED, SKIPPED, FAILED
  }

  void onSegment(ImmutableSegmentImpl segment, Outcome outcome);

  /// Called in finally after the attempt. An aborted attempt may have persisted some files already.
  void onComplete(boolean aborted);
}
