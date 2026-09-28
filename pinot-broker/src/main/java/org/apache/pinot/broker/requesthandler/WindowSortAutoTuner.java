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
package org.apache.pinot.broker.requesthandler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.planner.logical.WindowSortAutoPlan;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;


/// Learns when sender sorting is likely to beat the existing receiver sort for one ordered-window exchange.
/// The cache is bounded, expires observations, and stores only plan fingerprints, never SQL or result data.
/// A query with no sufficiently strong history uses the receiver-sort plan. Sessions are used once per query;
/// concurrent queries may safely read and update the shared cache.
public final class WindowSortAutoTuner {
  private static final int MIN_SENDERS = 4;
  private static final long MIN_ROWS = 400_000;
  private static final long MIN_SAMPLED_ROWS = MIN_SENDERS * 64L;
  private static final int REQUIRED_OBSERVATIONS = 2;
  private static final int NEGATIVE_OBSERVATIONS = -1;
  private static final int DISABLED_OBSERVATIONS = -2;
  private static final int NEGATIVE_SKIP_QUERIES = 7;
  private static final int RESAMPLE_EVERY = 8;
  private static final int SLOWDOWN_PERCENT = 15;

  private final Cache<ExchangeKey, Evidence> _observations = CacheBuilder.newBuilder()
      .maximumSize(1_024).expireAfterWrite(Duration.ofMinutes(30)).build();
  private final AtomicLong _nextGeneration = new AtomicLong();

  public Session newSession(long queryHash) {
    return new Session(queryHash);
  }

  /// A single query's planner decisions and, on successful completion, its receiver-stage observations.
  public final class Session implements WindowSortAutoPlan {
    private final long _queryHash;
    private final Map<ExchangeKey, Decision> _decisions = new HashMap<>();
    private final AtomicBoolean _observed = new AtomicBoolean();

    private Session(long queryHash) {
      _queryHash = queryHash;
    }

    @Override
    public boolean useSenderSort(int receiverStageId, int senderStageId, int inputHash, int collationHash) {
      return decisionFor(receiverStageId, senderStageId, inputHash, collationHash).senderSort();
    }

    @Override
    public boolean shouldProfile(int receiverStageId, int senderStageId, int inputHash, int collationHash) {
      return decisionFor(receiverStageId, senderStageId, inputHash, collationHash).profile();
    }

    private Decision decisionFor(int receiverStageId, int senderStageId, int inputHash, int collationHash) {
      ExchangeKey key = new ExchangeKey(_queryHash, receiverStageId, senderStageId, inputHash, collationHash);
      return _decisions.computeIfAbsent(key, ignored -> {
        Evidence evidence = _observations.getIfPresent(key);
        if (evidence == null) {
          Evidence initial = new Evidence(0, 0, _nextGeneration.incrementAndGet(), 0);
          Evidence existing = _observations.asMap().putIfAbsent(key, initial);
          evidence = existing != null ? existing : initial;
        }
        if (evidence.pendingProbe() != 0) {
          // A read does not refresh the TTL of an abandoned probe.
          return new Decision(false, false, evidence.generation(), 0);
        }
        if (evidence.qualifyingObservations() == DISABLED_OBSERVATIONS) {
          // Do not refresh the disabled entry's TTL; a new profile is possible only after it expires.
          return new Decision(false, false, evidence.generation(), 0);
        }
        if (evidence.qualifyingObservations() >= 0
            && evidence.qualifyingObservations() < REQUIRED_OBSERVATIONS) {
          return new Decision(false, true, evidence.generation(), 0);
        }
        AtomicReference<Decision> selected = new AtomicReference<>();
        _observations.asMap().computeIfPresent(key, (unused, current) -> {
          if (current.pendingProbe() != 0) {
            selected.set(new Decision(false, false, current.generation(), 0));
            return current;
          }
          if (current.qualifyingObservations() == DISABLED_OBSERVATIONS) {
            selected.set(new Decision(false, false, current.generation(), 0));
            return current;
          }
          if (current.qualifyingObservations() == NEGATIVE_OBSERVATIONS) {
            if (current.queriesSinceSample() > 0) {
              selected.set(new Decision(false, false, current.generation(), 0));
              return new Evidence(NEGATIVE_OBSERVATIONS, current.queriesSinceSample() - 1,
                  current.generation(), 0);
            }
            long generation = _nextGeneration.incrementAndGet();
            selected.set(new Decision(false, true, generation, generation));
            return new Evidence(NEGATIVE_OBSERVATIONS, 0, generation, generation);
          }
          if (current.qualifyingObservations() < REQUIRED_OBSERVATIONS) {
            selected.set(new Decision(false, true, current.generation(), 0));
            return current;
          }
          int queriesSinceSample = current.queriesSinceSample() + 1;
          if (queriesSinceSample == RESAMPLE_EVERY) {
            // Do not keep the sender choice ready while the receiver probe is in flight. If it fails or never
            // reports stats, subsequent queries remain on receiver sort until this entry expires.
            long generation = _nextGeneration.incrementAndGet();
            selected.set(new Decision(false, true, generation, generation));
            return new Evidence(1, 0, generation, generation, current.baselineNanos(), 0);
          }
          selected.set(new Decision(true, false, current.generation(), 0));
          return new Evidence(REQUIRED_OBSERVATIONS, queriesSinceSample, current.generation(), 0,
              current.baselineNanos(), current.slowSenderStreak());
        });
        return selected.get() != null ? selected.get() : new Decision(false, true, evidence.generation(), 0);
      });
    }

    /// Call only after a successful query. Stats are stage-indexed; ambiguous or incomplete evidence resets the
    /// candidate to the receiver-sort plan. A sender-sort query does not provide a new receiver-sort sample.
    public void observe(@Nullable List<MultiStageQueryStats.StageStats.Closed> stageStats) {
      observe(stageStats, 0);
    }

    /// The elapsed time spans dispatch through broker reduction, excluding planning and response construction.
    public void observe(@Nullable List<MultiStageQueryStats.StageStats.Closed> stageStats, long elapsedNanos) {
      if (!_observed.compareAndSet(false, true)) {
        return;
      }
      Map<Integer, Integer> candidateCountByStage = new HashMap<>();
      for (ExchangeKey key : _decisions.keySet()) {
        candidateCountByStage.merge(key.receiverStageId(), 1, Integer::sum);
      }
      for (Map.Entry<ExchangeKey, Decision> entry : _decisions.entrySet()) {
        Decision decision = entry.getValue();
        ExchangeKey key = entry.getKey();
        if (decision.senderSort()) {
          // End-to-end latency cannot attribute a slowdown to one of several AUTO exchanges in a query.
          if (_decisions.size() == 1) {
            observeSender(key, decision, elapsedNanos);
          }
          continue;
        }
        if (!decision.profile()) {
          continue;
        }
        ProfileResult result = candidateCountByStage.get(key.receiverStageId()) == 1
            ? profile(stageStats, key.receiverStageId())
            : ProfileResult.INCOMPLETE;
        Evidence snapshot = _observations.getIfPresent(key);
        if (snapshot == null || snapshot.generation() != decision.generation()
            || (snapshot.pendingProbe() != 0 && snapshot.pendingProbe() != decision.probeToken())
            || (snapshot.pendingProbe() == 0 && decision.probeToken() != 0)) {
          continue;
        }
        _observations.asMap().compute(key, (ignored, evidence) -> {
          if (evidence == null || evidence.generation() != decision.generation()) {
            return evidence;
          }
          if (evidence.pendingProbe() != 0) {
            if (decision.probeToken() != evidence.pendingProbe()) {
              return evidence;
            }
            if (result == ProfileResult.CANDIDATE) {
              int observations = evidence.qualifyingObservations() == NEGATIVE_OBSERVATIONS
                  ? 1
                  : REQUIRED_OBSERVATIONS;
              return new Evidence(observations, 0, _nextGeneration.incrementAndGet(), 0,
                  baselineAfterCandidate(evidence, elapsedNanos, observations), 0);
            }
            return result == ProfileResult.NON_CANDIDATE
                ? new Evidence(NEGATIVE_OBSERVATIONS, NEGATIVE_SKIP_QUERIES, _nextGeneration.incrementAndGet(), 0)
                : new Evidence(0, 0, _nextGeneration.incrementAndGet(), 0);
          }
          if (decision.probeToken() != 0) {
            return evidence;
          }
          if (result == ProfileResult.NON_CANDIDATE) {
            return new Evidence(NEGATIVE_OBSERVATIONS, NEGATIVE_SKIP_QUERIES, _nextGeneration.incrementAndGet(), 0);
          }
          if (result == ProfileResult.INCOMPLETE) {
            return new Evidence(0, 0, _nextGeneration.incrementAndGet(), 0);
          }
          int observations = Math.min(evidence.qualifyingObservations() + 1, REQUIRED_OBSERVATIONS);
          return new Evidence(observations, evidence.queriesSinceSample(), evidence.generation(), 0,
              baselineAfterCandidate(evidence, elapsedNanos, observations), 0);
        });
      }
    }

    private void observeSender(ExchangeKey key, Decision decision, long elapsedNanos) {
      if (elapsedNanos <= 0) {
        return;
      }
      Evidence snapshot = _observations.getIfPresent(key);
      if (snapshot == null || snapshot.generation() != decision.generation()
          || snapshot.qualifyingObservations() != REQUIRED_OBSERVATIONS || snapshot.pendingProbe() != 0
          || snapshot.baselineNanos() <= 0) {
        return;
      }
      _observations.asMap().computeIfPresent(key, (ignored, evidence) -> {
        if (evidence.generation() != decision.generation()
            || evidence.qualifyingObservations() != REQUIRED_OBSERVATIONS || evidence.pendingProbe() != 0
            || evidence.baselineNanos() <= 0) {
          return evidence;
        }
        long slowdownNanos = evidence.baselineNanos() * SLOWDOWN_PERCENT / 100;
        int slowStreak = elapsedNanos > evidence.baselineNanos() + slowdownNanos
            ? evidence.slowSenderStreak() + 1
            : 0;
        if (slowStreak >= 2) {
          return new Evidence(DISABLED_OBSERVATIONS, 0, _nextGeneration.incrementAndGet(), 0,
              evidence.baselineNanos(), 0);
        }
        return new Evidence(REQUIRED_OBSERVATIONS, evidence.queriesSinceSample(), evidence.generation(), 0,
            evidence.baselineNanos(), slowStreak);
      });
    }
  }

  private static long baselineAfterCandidate(Evidence evidence, long elapsedNanos, int observations) {
    if (observations == 1) {
      return Math.max(elapsedNanos, 0);
    }
    return evidence.baselineNanos() > 0 && elapsedNanos > 0
        ? Math.min(evidence.baselineNanos(), elapsedNanos)
        : evidence.baselineNanos();
  }

  private static ProfileResult profile(@Nullable List<MultiStageQueryStats.StageStats.Closed> stageStats,
      int receiverStageId) {
    if (stageStats == null || receiverStageId < 0 || receiverStageId >= stageStats.size()) {
      return ProfileResult.INCOMPLETE;
    }
    MultiStageQueryStats.StageStats.Closed closed = stageStats.get(receiverStageId);
    if (closed == null) {
      return ProfileResult.INCOMPLETE;
    }
    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStats = null;
    for (int i = 0; i <= closed.getLastOperatorIndex(); i++) {
      if (closed.getOperatorType(i) != MultiStageOperator.Type.MAILBOX_RECEIVE) {
        continue;
      }
      if (receiveStats != null) {
        return ProfileResult.INCOMPLETE;
      }
      @SuppressWarnings("unchecked")
      StatMap<BaseMailboxReceiveOperator.StatKey> stats =
          (StatMap<BaseMailboxReceiveOperator.StatKey>) closed.getOperatorStats(i);
      receiveStats = stats;
    }
    if (receiveStats == null) {
      return ProfileResult.INCOMPLETE;
    }
    if (receiveStats.getInt(BaseMailboxReceiveOperator.StatKey.FAN_IN) != MIN_SENDERS
        || receiveStats.getLong(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS) < MIN_ROWS) {
      return ProfileResult.NON_CANDIDATE;
    }
    long sampledRows = receiveStats.getLong(BaseMailboxReceiveOperator.StatKey.AUTO_SAMPLED_ROWS);
    if (sampledRows == 0) {
      // A large zero-sample query is indistinguishable from stats reported by an older receiver.
      return ProfileResult.INCOMPLETE;
    }
    if (receiveStats.getInt(BaseMailboxReceiveOperator.StatKey.AUTO_SAMPLE_STREAMS) != MIN_SENDERS
        || sampledRows < MIN_SAMPLED_ROWS) {
      // A successful receiver query with sampled rows but fewer than 65 rows on every sender cannot qualify.
      // Cache that negative result instead of profiling every execution of a skewed or sparse exchange.
      return ProfileResult.NON_CANDIDATE;
    }
    return receiveStats.getInt(BaseMailboxReceiveOperator.StatKey.AUTO_CANDIDATE_STREAMS) == MIN_SENDERS
        ? ProfileResult.CANDIDATE
        : ProfileResult.NON_CANDIDATE;
  }

  private enum ProfileResult {
    CANDIDATE, NON_CANDIDATE, INCOMPLETE
  }

  private record ExchangeKey(long queryHash, int receiverStageId, int senderStageId, int inputHash,
                             int collationHash) {
  }

  /// A negative observation uses -1 and counts down unprofiled receiver queries; -2 disables sender choice until TTL.
  /// A qualified observation counts sender queries toward its next receiver probe.
  private record Evidence(int qualifyingObservations, int queriesSinceSample, long generation, long pendingProbe,
                          long baselineNanos, int slowSenderStreak) {
    private Evidence(int qualifyingObservations, int queriesSinceSample, long generation, long pendingProbe) {
      this(qualifyingObservations, queriesSinceSample, generation, pendingProbe, 0, 0);
    }
  }

  private record Decision(boolean senderSort, boolean profile, long generation, long probeToken) {
  }
}
