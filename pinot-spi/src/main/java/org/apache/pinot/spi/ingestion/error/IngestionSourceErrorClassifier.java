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
package org.apache.pinot.spi.ingestion.error;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class IngestionSourceErrorClassifier {
  private static final int MAX_NODES = 1_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionSourceErrorClassifier.class);
  private static final long FAILURE_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final int MAX_LOGGED_FRAMES = 10;
  private static final Set<IngestionSourceErrorRule> REGISTERED_RULES = new CopyOnWriteArraySet<>();
  private static final Set<IngestionSourceErrorRule> REGISTERED_GENERIC_RULES = new CopyOnWriteArraySet<>();
  @VisibleForTesting
  static final AtomicLong LAST_FAILURE_LOG_MS = new AtomicLong();

  private IngestionSourceErrorClassifier() {
  }

  public static Optional<IngestionSourceErrorClassification> classify(Throwable failure) {
    try {
      Walk walk = new Walk();
      walk.visit(failure);
      Decision decided = walk.decide();
      if (decided == null) {
        return Optional.empty();
      }
      return Optional.of(new IngestionSourceErrorClassification(decided._verdict, decided._node, decided._evidence));
    } catch (Throwable t) {
      rethrowIfFatal(t);
      String failureClass = failure != null ? failure.getClass().getName() : null;
      warnAtMostOnceAMinute("Failed to classify an ingestion source failure: {}, caught: {}", failureClass, t);
      return Optional.empty();
    }
  }

  private static void warnAtMostOnceAMinute(String message, String subject, Throwable t) {
    long nowMs = System.currentTimeMillis();
    long lastLoggedMs = LAST_FAILURE_LOG_MS.get();
    if (nowMs - lastLoggedMs >= FAILURE_LOG_INTERVAL_MS && LAST_FAILURE_LOG_MS.compareAndSet(lastLoggedMs, nowMs)) {
      try {
        LOGGER.warn(message, subject, describe(t));
      } catch (Throwable logFailure) {
        rethrowIfFatal(logFailure);
      }
    }
  }

  private static String describe(Throwable t) {
    StringBuilder description = new StringBuilder(t.getClass().getName());
    StackTraceElement[] frames = t.getStackTrace();
    for (int i = 0; i < frames.length && i < MAX_LOGGED_FRAMES; i++) {
      description.append("\n\tat ").append(frames[i]);
    }
    return description.toString();
  }

  static void rethrowIfFatal(Throwable t) {
    if (t instanceof VirtualMachineError && !(t instanceof StackOverflowError)) {
      throw (VirtualMachineError) t;
    }
  }

  public static void registerRule(IngestionSourceErrorRule rule) {
    REGISTERED_RULES.add(rule);
  }

  public static void unregisterRule(IngestionSourceErrorRule rule) {
    REGISTERED_RULES.remove(rule);
  }

  public static void registerGenericRule(IngestionSourceErrorRule rule) {
    REGISTERED_GENERIC_RULES.add(rule);
  }

  public static void unregisterGenericRule(IngestionSourceErrorRule rule) {
    REGISTERED_GENERIC_RULES.remove(rule);
  }

  @Nullable
  private static IngestionSourceErrorVerdict decideRegistered(Set<IngestionSourceErrorRule> rules, Throwable node,
      List<Throwable> below, @Nullable IngestionSourceErrorEvidence evidence) {
    IngestionSourceErrorVerdict decided = null;
    for (IngestionSourceErrorRule rule : rules) {
      IngestionSourceErrorVerdict verdict;
      try {
        verdict = rule.decide(node, below, evidence);
      } catch (Throwable t) {
        rethrowIfFatal(t);
        warnAtMostOnceAMinute("Ingestion source error rule: {} failed, counting it as no decision, caught: {}",
            rule.getClass().getName(), t);
        continue;
      }
      if (verdict != null && (decided == null || PRECEDENCE.compare(verdict, decided) < 0)) {
        decided = verdict;
      }
    }
    return decided;
  }

  private static final Comparator<IngestionSourceErrorVerdict> PRECEDENCE =
      Comparator.comparingInt(IngestionSourceErrorClassifier::rank)
          .thenComparing(IngestionSourceErrorVerdict::getCode, Comparator.nullsLast(Comparator.naturalOrder()))
          .thenComparing(IngestionSourceErrorVerdict::getTarget, Comparator.nullsLast(Comparator.naturalOrder()));

  private static int rank(IngestionSourceErrorVerdict verdict) {
    return verdict.isNone() ? 0 : verdict.getCategory() == IngestionSourceErrorCategory.AUTH ? 1 : 2;
  }

  private static final class Decision {
    final IngestionSourceErrorVerdict _verdict;
    final Throwable _node;
    @Nullable
    final IngestionSourceErrorEvidence _evidence;

    Decision(IngestionSourceErrorVerdict verdict, Throwable node, @Nullable IngestionSourceErrorEvidence evidence) {
      _verdict = verdict;
      _node = node;
      _evidence = evidence;
    }
  }

  private static final class Chain {
    final List<Throwable> _nodes = new ArrayList<>();
    final List<IngestionSourceErrorEvidence> _direct = new ArrayList<>();
    final List<IngestionSourceErrorEvidence> _nearest = new ArrayList<>();
    int _firstBoundary = -1;
  }

  private static final class Walk {
    final Set<Throwable> _heads = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<Chain> _chains = new ArrayList<>();
    int _visited;
    boolean _hasBoundary;

    void visit(Throwable failure) {
      Deque<Throwable> pending = new ArrayDeque<>();
      pending.push(failure);
      while (!pending.isEmpty() && _visited < MAX_NODES) {
        Throwable head = pending.pop();
        if (!_heads.add(head)) {
          continue;
        }
        List<Throwable> suppressedHeads = visitChain(head);
        for (int i = suppressedHeads.size() - 1; i >= 0; i--) {
          pending.push(suppressedHeads.get(i));
        }
      }
    }

    private List<Throwable> visitChain(Throwable head) {
      Chain chain = new Chain();
      List<Throwable> suppressedHeads = new ArrayList<>();
      Set<Throwable> inChain = Collections.newSetFromMap(new IdentityHashMap<>());
      IngestionSourceErrorEvidence current = null;
      for (Throwable node = head; node != null && _visited < MAX_NODES && inChain.add(node);
          node = node.getCause()) {
        _visited++;
        IngestionSourceErrorEvidence own = null;
        for (Throwable suppressed : node.getSuppressed()) {
          if (suppressed instanceof IngestionSourceErrorEvidence) {
            own = own != null ? own : (IngestionSourceErrorEvidence) suppressed;
          } else {
            suppressedHeads.add(suppressed);
          }
        }
        if (own == null) {
          own = IngestionSourceErrorEvidence.ofUnsuppressible(node);
        }
        if (own != null) {
          current = own;
          if (own.isBoundary() && chain._firstBoundary < 0) {
            chain._firstBoundary = chain._nodes.size();
            _hasBoundary = true;
          }
        }
        chain._nodes.add(node);
        chain._direct.add(own);
        chain._nearest.add(current);
      }
      _chains.add(chain);
      return suppressedHeads;
    }

    @Nullable
    Decision decide() {
      Decision decided = null;
      for (Chain chain : _chains) {
        Decision decision = decide(chain);
        if (decision == null || decision._verdict.isNone()) {
          continue;
        }
        if (decided == null || (decision._verdict.getCategory() == IngestionSourceErrorCategory.AUTH
            && decided._verdict.getCategory() != IngestionSourceErrorCategory.AUTH)) {
          decided = decision;
        }
      }
      return decided;
    }

    @Nullable
    private Decision decide(Chain chain) {
      List<Throwable> nodes = Collections.unmodifiableList(chain._nodes);
      for (int i = nodes.size() - 1; i >= 0; i--) {
        if (!isEligible(chain, i)) {
          continue;
        }
        Throwable node = nodes.get(i);
        List<Throwable> below = nodes.subList(i + 1, nodes.size());
        IngestionSourceErrorEvidence direct = chain._direct.get(i);
        IngestionSourceErrorEvidence nearest = chain._nearest.get(i);
        IngestionSourceErrorVerdict verdict = direct != null ? IngestionSourceErrorRules.matchEvidence(direct) : null;
        if (verdict == null) {
          verdict = IngestionSourceErrorRules.matchSource(node, below, nearest);
        }
        if (verdict == null) {
          verdict = decideRegistered(REGISTERED_RULES, node, below, nearest);
        }
        if (verdict != null) {
          return new Decision(verdict, node, nearest);
        }
      }
      for (int i = nodes.size() - 1; i >= 0; i--) {
        if (!isEligible(chain, i)) {
          continue;
        }
        Throwable node = nodes.get(i);
        IngestionSourceErrorVerdict verdict = IngestionSourceErrorRules.matchGeneric(node);
        if (verdict == null) {
          verdict = decideRegistered(REGISTERED_GENERIC_RULES, node, nodes.subList(i + 1, nodes.size()),
              chain._nearest.get(i));
        }
        if (verdict != null) {
          return new Decision(verdict, node, chain._nearest.get(i));
        }
      }
      return null;
    }

    private boolean isEligible(Chain chain, int index) {
      return !_hasBoundary || (chain._firstBoundary >= 0 && index >= chain._firstBoundary);
    }
  }
}
