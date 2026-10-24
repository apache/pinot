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

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// The classifier's contract, independent of any SDK: how a tree's verdicts combine, how registered rules take part,
/// and that classification never fails or leaks. The per-SDK rules are pinned by fixture tests in the plugin modules
/// that ship each SDK.
public class IngestionSourceErrorClassifierTest {
  private final List<IngestionSourceErrorRule> _registered = new ArrayList<>();

  /// A source exception a registered rule recognises by name.
  private static class RefusedException extends RuntimeException {
    RefusedException(Throwable cause) {
      super("refused", cause);
    }
  }

  /// A source exception meaning the data is missing.
  private static class MissingException extends RuntimeException {
    MissingException() {
      super("missing");
    }
  }

  /// Our own failure under a source exception, e.g. a credential that failed to load.
  private static class OurFailureException extends IOException {
    OurFailureException() {
      super("ours");
    }
  }

  private static final IngestionSourceErrorRule TEST_RULES = (node, below, evidence) -> {
    if (IngestionSourceErrorRule.hasClass(node.getClass(), RefusedException.class.getName())) {
      return IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHORIZATION, "refused", null);
    }
    if (IngestionSourceErrorRule.hasClass(node.getClass(), MissingException.class.getName())) {
      return IngestionSourceErrorVerdict.sourceRead("missing", null);
    }
    if (IngestionSourceErrorRule.hasClass(node.getClass(), OurFailureException.class.getName())) {
      return IngestionSourceErrorVerdict.none("ours");
    }
    return null;
  };

  private void register(IngestionSourceErrorRule rule) {
    IngestionSourceErrorClassifier.registerRule(rule);
    _registered.add(rule);
  }

  @AfterMethod
  public void unregister() {
    _registered.forEach(IngestionSourceErrorClassifier::unregisterRule);
    _registered.clear();
  }

  private static Optional<IngestionSourceErrorCategory> categoryOf(Throwable failure) {
    return IngestionSourceErrorClassifier.classify(failure).map(IngestionSourceErrorClassification::getCategory);
  }

  @Test
  public void networkFailureUnderWrappersIsSourceRead() {
    Throwable failure = new RuntimeException("Pinot wrapper", new IllegalStateException(new ConnectException("x")));
    IngestionSourceErrorClassification classification = IngestionSourceErrorClassifier.classify(failure).get();
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
    assertEquals(classification.getExceptionClass(), ConnectException.class.getName());
  }

  @Test
  public void ourMetadataEndpointIsNotTheSource() {
    assertEquals(categoryOf(new RuntimeException(new UnknownHostException("169.254.169.254"))), Optional.empty());
  }

  @Test
  public void unrelatedFailureIsNotClassified() {
    assertEquals(categoryOf(new IllegalArgumentException("bad config")), Optional.empty());
    assertEquals(categoryOf(new RuntimeException(new InterruptedException())), Optional.empty());
  }

  @Test
  public void exclusionBeneathADecidingSourceExceptionWins() {
    register(TEST_RULES);
    // Our own failure is the root: a source class wrapping it can't reinstate a category.
    assertEquals(categoryOf(new RefusedException(new OurFailureException())), Optional.empty());
    // A source refusal is the root: our wrapper above it doesn't hide it.
    assertEquals(categoryOf(new RuntimeException(new OurFailureException().initCause(new RefusedException(null)))),
        Optional.of(IngestionSourceErrorCategory.AUTH));
  }

  @Test
  public void sourceRuleBeatsGenericNetworkRule() {
    register(TEST_RULES);
    assertEquals(categoryOf(new RefusedException(new ConnectException("x"))),
        Optional.of(IngestionSourceErrorCategory.AUTH));
  }

  @Test
  public void refusalHiddenInSuppressedFailuresWins() {
    register(TEST_RULES);
    Throwable failure = new MissingException();
    failure.addSuppressed(new RuntimeException("cleanup failed", new RefusedException(null)));
    assertEquals(categoryOf(failure), Optional.of(IngestionSourceErrorCategory.AUTH));
  }

  @Test
  public void registrationOrderNeverChangesTheOutcome() {
    IngestionSourceErrorRule readRule = (node, below, evidence) -> node instanceof MissingException
        ? IngestionSourceErrorVerdict.sourceRead("read", null) : null;
    IngestionSourceErrorRule authRule = (node, below, evidence) -> node instanceof MissingException
        ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.UNSPECIFIED, "auth", null) : null;
    IngestionSourceErrorRule noneRule = (node, below, evidence) -> node instanceof MissingException
        ? IngestionSourceErrorVerdict.none("none") : null;

    register(readRule);
    register(authRule);
    assertEquals(categoryOf(new MissingException()), Optional.of(IngestionSourceErrorCategory.AUTH));
    unregister();
    register(authRule);
    register(readRule);
    assertEquals(categoryOf(new MissingException()), Optional.of(IngestionSourceErrorCategory.AUTH));

    register(noneRule);
    assertEquals(categoryOf(new MissingException()), Optional.empty());
  }

  @Test
  public void registeredRuleNeverOverridesABuiltInDecision() {
    register((node, below, evidence) -> IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.UNSPECIFIED,
        "any", null));
    // A built-in rule decides the interrupt; the registered rule is consulted only for the wrapper above it.
    assertEquals(categoryOf(new InterruptedException()), Optional.empty());
  }

  @Test
  public void failingRuleCountsAsNoDecision() {
    register((node, below, evidence) -> {
      throw new IllegalStateException("rule bug");
    });
    register((node, below, evidence) -> {
      throw new NoClassDefFoundError("sdk class missing");
    });
    register(TEST_RULES);
    assertEquals(categoryOf(new MissingException()), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
  }

  @Test
  public void classifyNeverThrows() {
    Throwable hostile = new RuntimeException() {
      @Override
      public String getMessage() {
        throw new IllegalStateException("getMessage fails");
      }
    };
    assertEquals(categoryOf(new ConnectException().initCause(hostile)), Optional.of(
        IngestionSourceErrorCategory.SOURCE_READ));

    // A cause cycle and a tree that suppresses itself terminate.
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    a.addSuppressed(b);
    assertEquals(categoryOf(a), Optional.empty());
  }

  @Test
  public void recordedEvidenceDecidesAndIsNeverAFailureOfItsOwn() {
    Throwable unreachable = IngestionSourceErrorEvidence.attach(new RuntimeException("connect failed"),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.UNREACHABLE));
    assertEquals(categoryOf(new RuntimeException("wrapper", unreachable)),
        Optional.of(IngestionSourceErrorCategory.SOURCE_READ));

    Throwable missingTopic = IngestionSourceErrorEvidence.attach(new RuntimeException("Topic does not exist"),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorRules.KAFKA_UNKNOWN_TOPIC_OR_PARTITION));
    IngestionSourceErrorClassification classification = IngestionSourceErrorClassifier.classify(missingTopic).get();
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.SOURCE_READ);
    assertEquals(classification.getTarget(), IngestionSourceErrorVerdict.TARGET_BROKER);

    Throwable badKey = IngestionSourceErrorEvidence.attach(new IOException("Invalid PKCS#8 data."),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.LOCAL_PRECHECK));
    assertEquals(categoryOf(badKey), Optional.of(IngestionSourceErrorCategory.AUTH));

    // Our own setup failing beneath a network error: not the source.
    Throwable ourSetup = IngestionSourceErrorEvidence.attach(new IOException("trust store unreadable"),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.LOCAL_SETUP));
    assertEquals(categoryOf(new ConnectException().initCause(ourSetup)), Optional.empty());

    // Credential mode alone decides nothing.
    Throwable modeOnly = IngestionSourceErrorEvidence.attach(new IllegalStateException(),
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.STATIC));
    assertEquals(categoryOf(modeOnly), Optional.empty());
  }

  @Test
  public void classificationCarriesTheNearestCredentialModeAboveTheDecidingNode() {
    Throwable root = new ConnectException("x");
    Throwable plugin = IngestionSourceErrorEvidence.attach(new RuntimeException("plugin", root),
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.AMBIENT)
            .setRoleArnConfigured(true));
    IngestionSourceErrorClassification classification =
        IngestionSourceErrorClassifier.classify(new RuntimeException("Pinot", plugin)).get();
    assertEquals(classification.getCredentialMode(), IngestionSourceCredentialMode.AMBIENT);
    assertTrue(classification.isRoleArnConfigured());
  }

  @Test
  public void evidenceSurvivesAThrowableWithSuppressionDisabled() {
    Throwable unsuppressible = new RuntimeException("no suppression", null, false, false) {
    };
    IngestionSourceErrorEvidence evidence =
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.UNREACHABLE);
    assertSame(IngestionSourceErrorEvidence.attach(unsuppressible, evidence), unsuppressible);
    assertEquals(unsuppressible.getSuppressed().length, 0);
    assertEquals(IngestionSourceErrorEvidence.of(unsuppressible).getCode(), IngestionSourceErrorEvidence.UNREACHABLE);
    assertEquals(categoryOf(unsuppressible), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
  }

  @Test
  public void evidenceKeepsTheInnermostClientsRecord() {
    Throwable failure = new RuntimeException();
    IngestionSourceErrorEvidence.attach(failure,
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.STATIC));
    IngestionSourceErrorEvidence.attach(failure,
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.AMBIENT));
    assertEquals(IngestionSourceErrorEvidence.of(failure).getCredentialMode(), IngestionSourceCredentialMode.STATIC);
    assertEquals(failure.getSuppressed().length, 1);
  }

  @Test
  public void evidenceHoldsNoSecrets() {
    IngestionSourceErrorEvidence evidence = new IngestionSourceErrorEvidence()
        .setResource("https://key:s3cr3t@registry.example.com:8081/subjects?token=t0k3n")
        .setEvidence(403, "denied\nforged log line password=s3cr3t" + "x".repeat(200))
        .setTarget("schema registry");
    assertEquals(evidence.getResource(), "https://registry.example.com:8081");
    assertEquals(evidence.getTarget(), "schema_registry");
    String code = evidence.getCode();
    assertTrue(code.length() <= 64, code);
    assertFalse(code.contains("\n"), code);
    assertEquals(new IngestionSourceErrorEvidence().setResource("broker-1:9092 SASL_SSL").getResource(), "unknown");

    Throwable failure = IngestionSourceErrorEvidence.attach(new RuntimeException(), evidence);
    String printed = IngestionSourceErrorEvidence.of(failure).getMessage();
    assertFalse(printed.contains("t0k3n"), printed);
    assertFalse(printed.contains("key:"), printed);
    assertNull(IngestionSourceErrorEvidence.of(new RuntimeException()));
  }
}
