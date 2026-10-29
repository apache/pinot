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


/// Folds a failure to reach an ingestion source into [IngestionSourceErrorCategory#AUTH] or
/// [IngestionSourceErrorCategory#SOURCE_READ], from the source's own verdict on our request. Status codes and error
/// codes are only evidence for that verdict, read per source.
///
/// The exception tree is split into chains: the failure with its causes, and each suppressed exception with its own
/// causes. When an exception in the tree carries boundary evidence ([IngestionSourceErrorEvidence#setBoundary]), only
/// the parts of chains at or beneath a boundary are read; otherwise the caller vouches that the whole failure came
/// from the source. Within a chain:
///
/// 1. walking from the root cause up, the first source-specific rule that decides wins. A rule that says the failure
///    is not the source's verdict (our clock, our ambient identity, a credential that failed to load) therefore
///    decides only when no source exception sits beneath it, and a wrapper above it can't reinstate a category;
/// 2. generic rules, which read the transport rather than a source's answer (`java.net` / `javax.net.ssl`, and any
///    registered with [#registerGenericRule]), apply only when no source rule decided the chain.
///
/// Across chains [IngestionSourceErrorCategory#AUTH] beats [IngestionSourceErrorCategory#SOURCE_READ]: a refusal
/// recorded as a suppressed exception still surfaces.
///
/// Built-in rules cover kafka-clients, the Confluent schema-registry client, the AWS SDK v2, the Pulsar client and
/// admin APIs, gRPC and gax, Kerberos and OAuth logins, and JDK network types. They match classes by name anywhere in
/// the hierarchy, because SDKs load in plugin classloaders, sometimes in two versions at once. They read codes
/// reflectively and fail closed. A plugin for another SDK adds rules with [#registerRule].
///
/// [#classify] never throws, apart from rethrowing the JVM's own fatal errors (`OutOfMemoryError`, `InternalError`).
/// Its only side effect is a WARN, at most once a minute, when a rule or the classification itself fails.
public final class IngestionSourceErrorClassifier {
  // Bounds the walk over pathological exception graphs; real failures are a few dozen nodes.
  private static final int MAX_NODES = 1_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionSourceErrorClassifier.class);
  // Classification runs on every failed fetch or row, so a broken rule fails just as often
  private static final long FAILURE_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  // Enough to find the failing code
  private static final int MAX_LOGGED_FRAMES = 10;
  private static final Set<IngestionSourceErrorRule> REGISTERED_RULES = new CopyOnWriteArraySet<>();
  private static final Set<IngestionSourceErrorRule> REGISTERED_GENERIC_RULES = new CopyOnWriteArraySet<>();
  @VisibleForTesting
  static final AtomicLong LAST_FAILURE_LOG_MS = new AtomicLong();

  private IngestionSourceErrorClassifier() {
  }

  /// Classifies `failure`, or returns empty when it is not the source's verdict on our request.
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
      // Observation only: a classification bug, or a hostile exception, must never change how the failure itself is
      // handled.
      rethrowIfFatal(t);
      // The class only: the failure's message is the caller's to log
      String failureClass = failure != null ? failure.getClass().getName() : null;
      warnAtMostOnceAMinute("Failed to classify an ingestion source failure: {}, caught: {}", failureClass, t);
      return Optional.empty();
    }
  }

  /// Logs a failure of classification itself, at most once a minute across all of them. Describing a hostile exception
  /// can fail in turn, which must not escape either.
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

  /// The class and first stack frames of what classification caught, without its message, cause or suppressed
  /// exceptions: any of them can carry the classified failure, whose message is the caller's to log.
  private static String describe(Throwable t) {
    StringBuilder description = new StringBuilder(t.getClass().getName());
    StackTraceElement[] frames = t.getStackTrace();
    for (int i = 0; i < frames.length && i < MAX_LOGGED_FRAMES; i++) {
      description.append("\n\tat ").append(frames[i]);
    }
    return description.toString();
  }

  /// Running out of memory or a broken JVM is not ours to swallow; a stack overflow from a hostile exception is.
  static void rethrowIfFatal(Throwable t) {
    if (t instanceof VirtualMachineError && !(t instanceof StackOverflowError)) {
      throw (VirtualMachineError) t;
    }
  }

  /// Adds rules for an SDK Pinot doesn't ship. Registering the same rule twice has no effect, and the order rules are
  /// registered in never changes an outcome.
  public static void registerRule(IngestionSourceErrorRule rule) {
    REGISTERED_RULES.add(rule);
  }

  public static void unregisterRule(IngestionSourceErrorRule rule) {
    REGISTERED_RULES.remove(rule);
  }

  /// Adds generic rules: ones that read the transport rather than a source's answer, such as a file missing under a
  /// path the caller knows is remote. Like the built-in network rules, they apply to a chain only when no source rule,
  /// built-in or registered, decided it, and after the built-in network rules on each exception.
  public static void registerGenericRule(IngestionSourceErrorRule rule) {
    REGISTERED_GENERIC_RULES.add(rule);
  }

  public static void unregisterGenericRule(IngestionSourceErrorRule rule) {
    REGISTERED_GENERIC_RULES.remove(rule);
  }

  /// The decision of the registered rules on `node`. When they disagree, "not the source's verdict" beats
  /// [IngestionSourceErrorCategory#AUTH], which beats [IngestionSourceErrorCategory#SOURCE_READ], and remaining ties
  /// break on sub-reason, code and target, so registration order never matters.
  @Nullable
  private static IngestionSourceErrorVerdict decideRegistered(Set<IngestionSourceErrorRule> rules, Throwable node,
      List<Throwable> below, @Nullable IngestionSourceErrorEvidence evidence) {
    IngestionSourceErrorVerdict decided = null;
    for (IngestionSourceErrorRule rule : rules) {
      IngestionSourceErrorVerdict verdict;
      try {
        verdict = rule.decide(node, below, evidence);
      } catch (Throwable t) {
        // A rule that throws anything, even a checked exception it smuggles out, counts as no decision.
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
          .thenComparing(IngestionSourceErrorVerdict::getSubReason, Comparator.nullsLast(Comparator.naturalOrder()))
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

  /// One chain of the tree: an exception and its causes, with the evidence each node carries and the evidence nearest
  /// above it.
  private static final class Chain {
    final List<Throwable> _nodes = new ArrayList<>();
    final List<IngestionSourceErrorEvidence> _direct = new ArrayList<>();
    final List<IngestionSourceErrorEvidence> _nearest = new ArrayList<>();
    // The first node with a boundary: it and every node beneath it are within the boundary
    int _firstBoundary = -1;
  }

  private static final class Walk {
    final Set<Throwable> _heads = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<Chain> _chains = new ArrayList<>();
    int _visited;
    boolean _hasBoundary;

    /// Decides the chain starting at `failure`, then every chain hanging off it as a suppressed exception, depth
    /// first. A work stack rather than recursion: suppressed exceptions can nest arbitrarily deep.
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

    /// Records the chain starting at `head` and returns the heads of the chains suppressed along it. Chains may share
    /// causes, and each reads a shared cause under its own evidence, so only a cycle within the chain ends it early.
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

    /// Decides every chain visited and returns the first AUTH decision, else the first SOURCE_READ one. In a tree with
    /// a boundary, only the exceptions within one are read.
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
      // Rules read the nodes beneath theirs but never change them
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
