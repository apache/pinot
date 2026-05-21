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
package org.apache.pinot.query.service.dispatch.streaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.plan.MultiStageStatsTreeDecoder;
import org.apache.pinot.query.runtime.plan.StageStatsTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker-side per-query state for the {@code SubmitWithStream} dispatch path. Owns the per-stage tree accumulator,
 * the outstanding-opchain count, the per-stage coverage counters, and the set of open server streams.
 *
 * <p>Concurrency model — all mutating methods acquire the per-session lock, so the accumulator and counters need no
 * additional internal synchronization. gRPC client {@code onNext} callbacks land on I/O threads and call into this
 * session directly. Stat decoding (proto → {@link StageStatsTreeNode}) is done <em>outside</em> the lock to minimise
 * lock hold time; only the map mutations that update the accumulator are performed under the lock.
 *
 * <p>Completion semantics — {@link #awaitCompletion(long, TimeUnit)} returns {@code true} as soon as every expected
 * opchain has reported (early completion), and {@code false} if the timeout fires first. The dispatcher should call
 * it <strong>only after</strong> the broker receiving mailbox has finished, so that a successful return means both
 * "data done" and "stats fully accounted for". When it returns {@code false} the per-stage coverage exposes which
 * stages are missing.
 */
public class StreamingQuerySession {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingQuerySession.class);

  private final long _requestId;
  private final int _expectedOpChains;
  private final CountDownLatch _completionLatch;
  /**
   * Guards {@link #_stageAccumulator}, {@link #_respondedByStage}, {@link #_mergeFailedByStage},
   * {@link #_openStreams}, and {@link #_peerErrorObserved}. Lock hold time is proportional to the merge work (a few
   * map operations), not to proto decode; see {@link #recordOpChainComplete} for why decode is done outside.
   *
   * <p>If lock contention becomes a bottleneck at high QPS, a virtual-thread actor (one VT per query draining from
   * an {@code ArrayBlockingQueue}, with gRPC I/O threads simply enqueuing) would eliminate the lock entirely and
   * avoid any contention between concurrent inbound callbacks.
   */
  private final ReentrantLock _lock = new ReentrantLock();

  /** Per-stage merged accumulator. Mutated under {@link #_lock}. */
  private final Map<Integer, StageStatsTreeNode> _stageAccumulator = new HashMap<>();
  /** Per-stage count of opchains that have responded successfully and merged cleanly. */
  private final Map<Integer, Integer> _respondedByStage = new HashMap<>();
  /** Per-stage count of opchains that responded but the broker couldn't merge their payload. */
  private final Map<Integer, Integer> _mergeFailedByStage = new HashMap<>();

  /** Set of open server streams. Iteration order is insertion order so cancel fan-out is deterministic. */
  private final Set<StreamingServerHandle> _openStreams = new LinkedHashSet<>();

  /** True after the first peer error (success=false OpChainComplete or stream onError). Used to trigger fan-out
   * cancel idempotently. */
  private boolean _peerErrorObserved = false;

  public StreamingQuerySession(long requestId, int expectedOpChains) {
    _requestId = requestId;
    _expectedOpChains = expectedOpChains;
    _completionLatch = new CountDownLatch(expectedOpChains);
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getExpectedOpChains() {
    return _expectedOpChains;
  }

  /**
   * Registers an open server stream so the session can iterate them later for fan-out cancel. Must be called by the
   * dispatcher when the {@code SubmitWithStream} call is opened.
   */
  public void registerStream(StreamingServerHandle stream) {
    _lock.lock();
    try {
      _openStreams.add(stream);
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Removes a stream from the open-streams set. Called when the server emits {@code ServerDone} (clean close) or the
   * stream errors. Idempotent.
   */
  public void unregisterStream(StreamingServerHandle stream) {
    _lock.lock();
    try {
      _openStreams.remove(stream);
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Records an {@link Worker.OpChainComplete} message decoded from a server stream. Decrements the outstanding count
   * and merges the contained tree into the per-stage accumulator (or marks the stage {@code mergeFailed} on a shape
   * mismatch / decode failure). Also records {@code success=false} reports as peer errors so fan-out cancel can fire.
   *
   * <p>Decoding (proto → {@link StageStatsTreeNode}) is performed <em>before</em> acquiring {@link #_lock} because
   * the input proto is immutable and {@link MultiStageStatsTreeDecoder.Decoded} is a fresh allocation. Only the map
   * mutations are done under the lock, which keeps lock hold time proportional to the (small) merge work rather than
   * the full recursive decode.
   */
  public void recordOpChainComplete(Worker.OpChainComplete message) {
    int stageId = message.getStageId();
    boolean isSuccess = message.getSuccess();
    Worker.MultiStageStatsTree statsTree = message.getStats();

    // Decode outside the lock — proto is immutable, Decoded is a fresh allocation with no shared state.
    MultiStageStatsTreeDecoder.Decoded decoded = null;
    MultiStageStatsTreeDecoder.DecodeFailedException decodeError = null;
    if (statsTree.hasCurrentStage()) {
      try {
        decoded = MultiStageStatsTreeDecoder.decode(statsTree);
      } catch (MultiStageStatsTreeDecoder.DecodeFailedException e) {
        decodeError = e;
      }
    }

    boolean shouldFanOutCancel = false;
    _lock.lock();
    try {
      if (!isSuccess) {
        if (!_peerErrorObserved) {
          _peerErrorObserved = true;
          shouldFanOutCancel = true;
        }
      }
      if (decodeError != null) {
        LOGGER.warn("Decode failed for opchain stage={} worker={} on request {}: {}",
            stageId, message.getWorkerId(), _requestId, decodeError.getMessage());
        incrementLocked(_mergeFailedByStage, stageId);
      } else if (decoded != null) {
        mergeIntoAccumulatorLocked(decoded.getCurrentStageId(), decoded.getCurrentStage());
        for (Map.Entry<Integer, StageStatsTreeNode> upstream : decoded.getUpstreamStages().entrySet()) {
          mergeIntoAccumulatorLocked(upstream.getKey(), upstream.getValue());
        }
        incrementLocked(_respondedByStage, stageId);
      } else {
        // Successful opchain with no stats tree (e.g. empty plan). Still counts as responded.
        incrementLocked(_respondedByStage, stageId);
      }
    } finally {
      _lock.unlock();
    }
    _completionLatch.countDown();
    if (shouldFanOutCancel) {
      fanOutCancel();
    }
  }

  private void mergeIntoAccumulatorLocked(int stageId, StageStatsTreeNode incoming) {
    StageStatsTreeNode existing = _stageAccumulator.get(stageId);
    if (existing == null) {
      _stageAccumulator.put(stageId, incoming);
      return;
    }
    try {
      existing.merge(incoming);
    } catch (StageStatsTreeNode.ShapeMismatchException e) {
      // StageStatsTreeNode.merge mutates _statMap before recursing into children, so a ShapeMismatchException
      // thrown during child recursion leaves the existing node in a partially-accumulated state. Remove it so
      // subsequent opchains for this stage do not merge into corrupt state.
      _stageAccumulator.remove(stageId);
      LOGGER.warn("Shape mismatch merging stage {} on request {}: {}", stageId, _requestId, e.getMessage());
      incrementLocked(_mergeFailedByStage, stageId);
    }
  }

  private static void incrementLocked(Map<Integer, Integer> counter, int stageId) {
    counter.merge(stageId, 1, Integer::sum);
  }

  /**
   * Records a transport-level error on one of the server streams (gRPC {@code onError}). Drains exactly
   * {@code remainingExpected} entries from the latch (the number of opchains that will not now report) and triggers
   * fan-out cancel if not already triggered.
   *
   * <p>The caller must pass the precise per-server remaining count rather than a fixed 1, because a single stream can
   * carry multiple opchains (one per worker per stage). Passing an incorrect count either under-drains (causes
   * {@link #awaitCompletion} to block until the query deadline) or over-drains (causes the latch to reach zero before
   * all reports have arrived).
   */
  public void recordStreamError(StreamingServerHandle stream, @Nullable Throwable error, int remainingExpected) {
    boolean shouldFanOutCancel = false;
    _lock.lock();
    try {
      _openStreams.remove(stream);
      if (!_peerErrorObserved) {
        _peerErrorObserved = true;
        shouldFanOutCancel = true;
      }
    } finally {
      _lock.unlock();
    }
    for (int i = 0; i < remainingExpected; i++) {
      _completionLatch.countDown();
    }
    LOGGER.warn("Stream error on request {} draining {} pending: {}",
        _requestId, remainingExpected, error == null ? "<null>" : error.getMessage());
    if (shouldFanOutCancel) {
      fanOutCancel();
    }
  }

  /**
   * Sends {@code BrokerToServer.cancel} on every open server stream. Used on the first peer error observed and when
   * the broker's data mailbox reports a processing exception. Failures are swallowed — cancel is best-effort.
   * Idempotent w.r.t. concurrent calls.
   */
  public void fanOutCancel() {
    Set<StreamingServerHandle> snapshot;
    _lock.lock();
    try {
      snapshot = new LinkedHashSet<>(_openStreams);
    } finally {
      _lock.unlock();
    }
    for (StreamingServerHandle stream : snapshot) {
      try {
        stream.cancel(_requestId);
      } catch (Throwable t) {
        LOGGER.warn("Failed to fan out cancel on request {}", _requestId, t);
      }
    }
  }

  /**
   * Blocks the calling thread until either every expected opchain has reported, or the timeout fires.
   *
   * <p>Returns {@code true} when all opchains reported before the timeout (early completion is the common case in
   * stream mode). Returns {@code false} when the timeout fired first; the per-stage coverage exposed via
   * {@link #snapshotCoverage()} indicates which stages are missing or had merge failures.
   *
   * <p>The dispatcher should only call this <strong>after</strong> the broker receiving mailbox has finished. That
   * way a {@code true} return means both "data done" and "stats fully accounted for".
   */
  public boolean awaitCompletion(long timeout, TimeUnit unit)
      throws InterruptedException {
    return _completionLatch.await(timeout, unit);
  }

  /**
   * Returns a snapshot of the per-stage coverage. Stage ids that received any responses (successful or
   * merge-failed) appear in the map; missing stages are computed by the caller against the expected total.
   */
  public Coverage snapshotCoverage() {
    _lock.lock();
    try {
      return new Coverage(new HashMap<>(_respondedByStage), new HashMap<>(_mergeFailedByStage),
          new HashMap<>(_stageAccumulator));
    } finally {
      _lock.unlock();
    }
  }

  /** Snapshot of the accumulator and per-stage counters at a point in time. */
  public static final class Coverage {
    private final Map<Integer, Integer> _respondedByStage;
    private final Map<Integer, Integer> _mergeFailedByStage;
    private final Map<Integer, StageStatsTreeNode> _stageAccumulator;

    public Coverage(Map<Integer, Integer> respondedByStage, Map<Integer, Integer> mergeFailedByStage,
        Map<Integer, StageStatsTreeNode> stageAccumulator) {
      _respondedByStage = Collections.unmodifiableMap(respondedByStage);
      _mergeFailedByStage = Collections.unmodifiableMap(mergeFailedByStage);
      _stageAccumulator = Collections.unmodifiableMap(stageAccumulator);
    }

    public Map<Integer, Integer> getRespondedByStage() {
      return _respondedByStage;
    }

    public Map<Integer, Integer> getMergeFailedByStage() {
      return _mergeFailedByStage;
    }

    public Map<Integer, StageStatsTreeNode> getStageAccumulator() {
      return _stageAccumulator;
    }
  }

  /** Returns the number of opchains still expected to report. Visible for tests. */
  public long getOutstandingCount() {
    return _completionLatch.getCount();
  }
}
