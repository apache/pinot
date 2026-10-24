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

import java.util.ArrayList;
import java.util.Collections;
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
/// [#classify] is pure and never throws.
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
      walk.visitChain(failure);
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
    } catch (RuntimeException | LinkageError | AssertionError e) {
      // Observation only: a classification bug must never change how the failure itself is handled.
      return Optional.empty();
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
  /// [IngestionSourceErrorCategory#AUTH], which beats [IngestionSourceErrorCategory#SOURCE_READ], so registration
  /// order never matters.
  @Nullable
  private static IngestionSourceErrorVerdict decideRegistered(Throwable node, List<Throwable> below,
      @Nullable IngestionSourceErrorEvidence evidence) {
    IngestionSourceErrorVerdict decided = null;
    for (IngestionSourceErrorRule rule : REGISTERED_RULES) {
      IngestionSourceErrorVerdict verdict;
      try {
        verdict = rule.decide(node, below, evidence);
      } catch (RuntimeException | LinkageError | AssertionError e) {
        continue;
      }
      if (verdict != null && (decided == null || rank(verdict) < rank(decided))) {
        decided = verdict;
      }
    }
    return decided;
  }

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
    final Set<Throwable> _seen = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<Decision> _decisions = new ArrayList<>();

    /// Decides the chain starting at `head`, then every chain hanging off it as a suppressed exception.
    void visitChain(Throwable head) {
      List<Throwable> chain = new ArrayList<>();
      List<IngestionSourceErrorEvidence> evidence = new ArrayList<>();
      List<Throwable> suppressedHeads = new ArrayList<>();
      IngestionSourceErrorEvidence nearest = null;
      for (Throwable node = head; node != null && _seen.size() < MAX_NODES && _seen.add(node);
          node = node.getCause()) {
        IngestionSourceErrorEvidence direct = IngestionSourceErrorEvidence.of(node);
        if (direct != null) {
          nearest = direct;
        }
        chain.add(node);
        evidence.add(nearest);
        for (Throwable suppressed : node.getSuppressed()) {
          if (!(suppressed instanceof IngestionSourceErrorEvidence)) {
            suppressedHeads.add(suppressed);
          }
        }
      }
      Decision decision = decide(chain, evidence);
      if (decision != null) {
        _decisions.add(decision);
      }
      for (Throwable suppressed : suppressedHeads) {
        visitChain(suppressed);
      }
    }

    @Nullable
    private static Decision decide(List<Throwable> chain, List<IngestionSourceErrorEvidence> evidence) {
      for (int i = chain.size() - 1; i >= 0; i--) {
        Throwable node = chain.get(i);
        List<Throwable> below = chain.subList(i + 1, chain.size());
        IngestionSourceErrorEvidence direct = IngestionSourceErrorEvidence.of(node);
        IngestionSourceErrorVerdict verdict =
            direct != null && direct.hasEvidence() ? IngestionSourceErrorRules.matchEvidence(direct) : null;
        if (verdict == null) {
          verdict = IngestionSourceErrorRules.matchSource(node, below, evidence.get(i));
        }
        if (verdict == null) {
          verdict = decideRegistered(node, below, evidence.get(i));
        }
        if (verdict != null) {
          return new Decision(verdict, node, evidence.get(i));
        }
      }
      for (int i = chain.size() - 1; i >= 0; i--) {
        IngestionSourceErrorVerdict verdict = IngestionSourceErrorRules.matchGeneric(chain.get(i));
        if (verdict != null) {
          return new Decision(verdict, chain.get(i), evidence.get(i));
        }
      }
      return null;
    }
  }
}
