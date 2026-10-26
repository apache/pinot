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
import javax.annotation.Nullable;


/// Folds a failure to reach an ingestion source into [IngestionSourceErrorCategory#AUTH] or
/// [IngestionSourceErrorCategory#SOURCE_READ], from the source's own verdict on our request. Status codes and error
/// codes are only evidence for that verdict, read per source.
///
/// The exception tree is split into chains: the failure with its causes, and each suppressed exception with its own
/// causes. Within a chain:
///
/// 1. walking from the root cause up, the first source-specific rule that decides wins. A rule that says the failure
///    is not the source's verdict (our clock, our ambient identity, a credential that failed to load) therefore
///    decides only when no source exception sits beneath it, and a wrapper above it can't reinstate a category;
/// 2. generic `java.net` / `javax.net.ssl` rules apply only when no source rule decided the chain.
///
/// Across chains [IngestionSourceErrorCategory#AUTH] beats [IngestionSourceErrorCategory#SOURCE_READ]: a refusal
/// recorded as a suppressed exception still surfaces.
///
/// Built-in rules cover kafka-clients, the Confluent schema-registry client, the AWS SDK v2, the Pulsar client and
/// admin APIs, gRPC and gax, Kerberos and OAuth logins, and JDK network types. They match classes by name anywhere in
/// the hierarchy, because SDKs load in plugin classloaders, sometimes in two versions at once. They read codes
/// reflectively and fail closed. A plugin for another SDK adds rules with [#registerRule].
///
/// [#classify] is pure and never throws, apart from rethrowing the JVM's own fatal errors (`OutOfMemoryError`,
/// `InternalError`).
public final class IngestionSourceErrorClassifier {
  // Bounds the walk over pathological exception graphs; real failures are a few dozen nodes.
  private static final int MAX_NODES = 1_000;
  private static final Set<IngestionSourceErrorRule> REGISTERED_RULES = new CopyOnWriteArraySet<>();

  private IngestionSourceErrorClassifier() {
  }

  /// Classifies `failure`, or returns empty when it is not the source's verdict on our request.
  public static Optional<IngestionSourceErrorClassification> classify(Throwable failure) {
    try {
      Walk walk = new Walk();
      walk.visit(failure);
      Decision decided = null;
      for (Decision decision : walk._decisions) {
        if (decision._verdict.getCategory() == IngestionSourceErrorCategory.AUTH) {
          decided = decision;
          break;
        }
        if (decided == null && decision._verdict.getCategory() == IngestionSourceErrorCategory.SOURCE_READ) {
          decided = decision;
        }
      }
      if (decided == null) {
        return Optional.empty();
      }
      return Optional.of(new IngestionSourceErrorClassification(decided._verdict, decided._node.getClass().getName(),
          decided._evidence));
    } catch (Throwable t) {
      // Observation only: a classification bug, or a hostile exception, must never change how the failure itself is
      // handled.
      rethrowIfFatal(t);
      return Optional.empty();
    }
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

  /// The decision of the registered rules on `node`. When they disagree, "not the source's verdict" beats
  /// [IngestionSourceErrorCategory#AUTH], which beats [IngestionSourceErrorCategory#SOURCE_READ], and remaining ties
  /// break on sub-reason, code and target, so registration order never matters.
  @Nullable
  private static IngestionSourceErrorVerdict decideRegistered(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
    IngestionSourceErrorVerdict decided = null;
    for (IngestionSourceErrorRule rule : REGISTERED_RULES) {
      IngestionSourceErrorVerdict verdict;
      try {
        verdict = rule.decide(node, below, evidence);
      } catch (Throwable t) {
        // A rule that throws anything, even a checked exception it smuggles out, counts as no decision.
        rethrowIfFatal(t);
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

  private static final class Walk {
    final Set<Throwable> _heads = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<Decision> _decisions = new ArrayList<>();
    int _visited;

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

    /// Decides the chain starting at `head` and returns the heads of the chains suppressed along it. Chains may share
    /// causes, and each reads a shared cause under its own evidence, so only a cycle within the chain ends it early.
    private List<Throwable> visitChain(Throwable head) {
      List<Throwable> chain = new ArrayList<>();
      List<IngestionSourceErrorEvidence> direct = new ArrayList<>();
      List<IngestionSourceErrorEvidence> nearest = new ArrayList<>();
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
        }
        chain.add(node);
        direct.add(own);
        nearest.add(current);
      }
      Decision decision = decide(chain, direct, nearest);
      if (decision != null) {
        _decisions.add(decision);
      }
      return suppressedHeads;
    }

    @Nullable
    private static Decision decide(List<Throwable> chain, List<IngestionSourceErrorEvidence> direct,
        List<IngestionSourceErrorEvidence> nearest) {
      for (int i = chain.size() - 1; i >= 0; i--) {
        Throwable node = chain.get(i);
        List<Throwable> below = chain.subList(i + 1, chain.size());
        IngestionSourceErrorVerdict verdict =
            direct.get(i) != null ? IngestionSourceErrorRules.matchEvidence(direct.get(i)) : null;
        if (verdict == null) {
          verdict = IngestionSourceErrorRules.matchSource(node, below, nearest.get(i));
        }
        if (verdict == null) {
          verdict = decideRegistered(node, below, nearest.get(i));
        }
        if (verdict != null) {
          return new Decision(verdict, node, nearest.get(i));
        }
      }
      for (int i = chain.size() - 1; i >= 0; i--) {
        IngestionSourceErrorVerdict verdict = IngestionSourceErrorRules.matchGeneric(chain.get(i));
        if (verdict != null) {
          return new Decision(verdict, chain.get(i), nearest.get(i));
        }
      }
      return null;
    }
  }
}
