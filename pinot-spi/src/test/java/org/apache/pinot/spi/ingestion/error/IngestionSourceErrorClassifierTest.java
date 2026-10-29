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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLHandshakeException;
import javax.security.auth.login.LoginException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
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

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> E sneaky(Throwable t)
      throws E {
    throw (E) t;
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
  public void peerRejectingOurCertificateIsAuthOverAnyTransport() {
    // A schema registry, Pulsar or any HTTP source with mutual TLS, as for a Kafka broker
    IngestionSourceErrorClassification rejected = IngestionSourceErrorClassifier.classify(new RuntimeException(
        "Error retrieving Avro schema", new SSLHandshakeException("Received fatal alert: bad_certificate"))).get();
    assertEquals(rejected.getCategory(), IngestionSourceErrorCategory.AUTH);
    assertEquals(rejected.getSubReason(), IngestionSourceErrorSubReason.AUTHENTICATION);
    assertEquals(categoryOf(new RuntimeException(new SSLHandshakeException("Received fatal alert: access_denied"))),
        Optional.of(IngestionSourceErrorCategory.AUTH));
    // Any other handshake failure: the TLS connection to the endpoint failed
    assertEquals(categoryOf(new RuntimeException(new SSLHandshakeException("Received fatal alert: handshake_failure"))),
        Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
  }

  @Test
  public void ourMetadataEndpointIsNotTheSource() {
    // The HTTP client's connect failure names the endpoint; the JDK's own doesn't.
    assertEquals(categoryOf(new RuntimeException(
        new ConnectException("Connect to 169.254.169.254:80 [/169.254.169.254] failed: Connection refused"))),
        Optional.empty());
  }

  @Test
  public void ourOwnSocketExhaustionIsNotTheSource() {
    assertEquals(categoryOf(new RuntimeException(new BindException("Cannot assign requested address"))),
        Optional.empty());
    assertEquals(categoryOf(new RuntimeException(new SocketException("Too many open files"))), Optional.empty());
    assertEquals(categoryOf(new RuntimeException(new SocketException("Connection reset"))),
        Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
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
  public void causeSharedByTwoBranchesIsReadUnderEachBranchsEvidence() {
    // Whose credential was refused decides: our ambient identity is none, the customer's static key is AUTH.
    register((node, below, evidence) -> !(node instanceof RefusedException) ? null
        : evidence != null && evidence.getCredentialMode() == IngestionSourceCredentialMode.AMBIENT
            ? IngestionSourceErrorVerdict.none("ours")
            : IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHENTICATION, "refused", null));
    Throwable shared = new RefusedException(null);
    Throwable ambient = IngestionSourceErrorEvidence.attach(new RuntimeException("first attempt", shared),
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.AMBIENT));
    Throwable customer = IngestionSourceErrorEvidence.attach(new RuntimeException("second attempt", shared),
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.STATIC));
    for (List<Throwable> branches : List.of(List.of(ambient, customer), List.of(customer, ambient))) {
      Throwable failure = new RuntimeException("all attempts failed");
      branches.forEach(failure::addSuppressed);
      assertEquals(categoryOf(failure), Optional.of(IngestionSourceErrorCategory.AUTH));
    }
  }

  @Test
  public void genericRuleAppliesOnlyWhereNoSourceRuleDecidedTheChain() {
    // A file missing under a path the caller knows is remote: the transport's word, not a source's answer
    IngestionSourceErrorRule remoteFileMissing = (node, below, evidence) -> node instanceof FileNotFoundException
        ? IngestionSourceErrorVerdict.sourceRead("FileNotFoundException", null) : null;
    IngestionSourceErrorClassifier.registerGenericRule(remoteFileMissing);
    try {
      register(TEST_RULES);
      assertEquals(categoryOf(new RuntimeException(new FileNotFoundException("s3://bucket/input"))),
          Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
      // A source exception above it decides the chain, even our own failure that says it isn't the source's verdict
      assertEquals(categoryOf(new RefusedException(new FileNotFoundException("s3://bucket/input"))),
          Optional.of(IngestionSourceErrorCategory.AUTH));
      assertEquals(categoryOf(new OurFailureException().initCause(new FileNotFoundException("s3://bucket/input"))),
          Optional.empty());
    } finally {
      IngestionSourceErrorClassifier.unregisterGenericRule(remoteFileMissing);
    }
  }

  @Test
  public void registeredGenericRulesKeepTheContractOfTheOthers() {
    IngestionSourceErrorRule missingIsRead = (node, below, evidence) -> node instanceof FileNotFoundException
        ? IngestionSourceErrorVerdict.sourceRead("FileNotFoundException", null) : null;
    IngestionSourceErrorRule missingIsAuth = (node, below, evidence) -> node instanceof FileNotFoundException
        ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHORIZATION, "FileNotFoundException", null)
        : null;
    IngestionSourceErrorRule connectIsAuth = (node, below, evidence) -> node instanceof ConnectException
        ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.UNSPECIFIED, "ConnectException", null) : null;
    IngestionSourceErrorRule failing = (node, below, evidence) -> {
      throw new IllegalStateException("rule bug");
    };
    List<IngestionSourceErrorRule> rules = List.of(failing, missingIsRead, missingIsAuth, connectIsAuth);
    rules.forEach(IngestionSourceErrorClassifier::registerGenericRule);
    try {
      // A failing rule is no decision, and the stronger verdict wins, not the first registered
      assertEquals(categoryOf(new RuntimeException(new FileNotFoundException("s3://bucket/input"))),
          Optional.of(IngestionSourceErrorCategory.AUTH));
      // The built-in network rules decide an exception first
      assertEquals(categoryOf(new RuntimeException(new ConnectException("refused"))),
          Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
      // In a tree with a boundary, nothing outside it is read
      Throwable failure = new RuntimeException("task failed");
      failure.addSuppressed(sourceCall(new RuntimeException("source read failed", new IllegalStateException())));
      failure.addSuppressed(new RuntimeException("deep store read failed", new FileNotFoundException("/segments/a")));
      assertEquals(categoryOf(failure), Optional.empty());
    } finally {
      rules.forEach(IngestionSourceErrorClassifier::unregisterGenericRule);
    }
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
  public void registrationOrderNeverChangesTheDetailEither() {
    IngestionSourceErrorRule authenticationRule = (node, below, evidence) -> node instanceof MissingException
        ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHENTICATION, "b", null) : null;
    IngestionSourceErrorRule authorizationRule = (node, below, evidence) -> node instanceof MissingException
        ? IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.AUTHORIZATION, "a", null) : null;
    register(authenticationRule);
    register(authorizationRule);
    String first = IngestionSourceErrorClassifier.classify(new MissingException()).get().toString();
    unregister();
    register(authorizationRule);
    register(authenticationRule);
    assertEquals(IngestionSourceErrorClassifier.classify(new MissingException()).get().toString(), first);
  }

  @Test
  public void registeredRuleNeverOverridesABuiltInDecision() {
    register((node, below, evidence) -> IngestionSourceErrorVerdict.auth(IngestionSourceErrorSubReason.UNSPECIFIED,
        "any", null));
    // A built-in rule decides the interrupt at the root, so the registered rule's verdict on the wrapper above it is
    // never reached.
    assertEquals(categoryOf(new RuntimeException("wrapper", new InterruptedException())), Optional.empty());
  }

  @Test
  public void failingRuleCountsAsNoDecision() {
    register((node, below, evidence) -> {
      throw new IllegalStateException("rule bug");
    });
    register((node, below, evidence) -> {
      throw new NoClassDefFoundError("sdk class missing");
    });
    register((node, below, evidence) -> {
      throw new Error("plain error");
    });
    register(TEST_RULES);
    assertEquals(categoryOf(new MissingException()), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
  }

  @Test
  public void classifyNeverThrows() {
    // Rules read these classes' messages; a getMessage() that throws reads as no message.
    Throwable hostileNetwork = new UnknownHostException() {
      @Override
      public String getMessage() {
        throw new IllegalStateException("getMessage fails");
      }
    };
    assertEquals(categoryOf(new RuntimeException("wrapper", hostileNetwork)), Optional.of(
        IngestionSourceErrorCategory.SOURCE_READ));
    Throwable hostileLogin = new LoginException() {
      @Override
      public String getMessage() {
        throw new IllegalStateException("getMessage fails");
      }
    };
    assertEquals(categoryOf(new RuntimeException("wrapper", hostileLogin)), Optional.empty());

    // A rule that smuggles out a checked exception or overflows counts as no decision.
    register((node, below, evidence) -> {
      throw IngestionSourceErrorClassifierTest.<RuntimeException>sneaky(new IOException("checked"));
    });
    register((node, below, evidence) -> {
      throw new StackOverflowError();
    });
    assertEquals(categoryOf(new ConnectException("refused")), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
    // A getCause() that throws ends classification, never the caller.
    Throwable hostileCause = new RuntimeException("wrapper") {
      @Override
      public synchronized Throwable getCause() {
        throw new IllegalStateException("getCause fails");
      }
    };
    assertEquals(categoryOf(hostileCause), Optional.empty());

    // A cause cycle and a tree that suppresses itself terminate.
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    a.addSuppressed(b);
    assertEquals(categoryOf(a), Optional.empty());
  }

  @Test
  public void classificationFailuresWarnAtMostOnceAMinuteWithoutTheFailuresMessage() {
    String marker = "fixture-secret";
    // A rule that fails on every node, carrying the node's message in its own, its cause and a suppressed exception
    IngestionSourceErrorRule brokenRule = (node, below, evidence) -> {
      IllegalStateException failure = new IllegalStateException("rule failed on: " + node.getMessage(), node);
      failure.addSuppressed(new RuntimeException(node.getMessage()));
      throw failure;
    };
    register(brokenRule);
    // As in a fresh process: another test class in this JVM may have logged within the interval
    IngestionSourceErrorClassifier.LAST_FAILURE_LOG_MS.set(0);
    WarnCapture warnings = WarnCapture.attachTo(IngestionSourceErrorClassifier.class.getName());
    try {
      for (int i = 0; i < 1_000; i++) {
        assertEquals(categoryOf(new ConnectException(marker)), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
      }
      assertEquals(warnings._rendered.size(), 1, warnings._rendered.toString());
      String ruleWarning = warnings._rendered.get(0);
      assertTrue(ruleWarning.startsWith("Ingestion source error rule: " + brokenRule.getClass().getName() + " failed"),
          ruleWarning);
      assertTrue(ruleWarning.contains(IllegalStateException.class.getName()), ruleWarning);
      assertFalse(ruleWarning.contains(marker), ruleWarning);

      // Within the minute nothing more is logged; after it, the next failure is
      IngestionSourceErrorClassifier.LAST_FAILURE_LOG_MS.set(System.currentTimeMillis() - 50_000);
      categoryOf(new ConnectException(marker));
      assertEquals(warnings._rendered.size(), 1, warnings._rendered.toString());
      IngestionSourceErrorClassifier.LAST_FAILURE_LOG_MS.set(System.currentTimeMillis() - 70_000);
      categoryOf(new ConnectException(marker));
      assertEquals(warnings._rendered.size(), 2, warnings._rendered.toString());

      // The walk failing warns too, naming the failure's class only
      IngestionSourceErrorClassifier.LAST_FAILURE_LOG_MS.set(0);
      Throwable hostileCause = new RuntimeException(marker) {
        @Override
        public synchronized Throwable getCause() {
          throw new IllegalStateException(marker);
        }
      };
      assertEquals(categoryOf(hostileCause), Optional.empty());
      assertEquals(warnings._rendered.size(), 3, warnings._rendered.toString());
      String walkWarning = warnings._rendered.get(2);
      assertTrue(walkWarning.startsWith(
          "Failed to classify an ingestion source failure: " + hostileCause.getClass().getName()), walkWarning);
      assertFalse(walkWarning.contains(marker), walkWarning);
      // Nor does a null failure escape
      IngestionSourceErrorClassifier.LAST_FAILURE_LOG_MS.set(0);
      assertEquals(IngestionSourceErrorClassifier.classify(null), Optional.empty());
      assertEquals(warnings._rendered.size(), 4, warnings._rendered.toString());
    } finally {
      warnings.detach();
    }
  }

  /// Collects the WARN events of one logger, rendered with any throwable they carry.
  private static final class WarnCapture extends AbstractAppender {
    private static final PatternLayout RENDERING = PatternLayout.newBuilder().withPattern("%m%n%throwable").build();
    final List<String> _rendered = new CopyOnWriteArrayList<>();
    private final Logger _logger;

    private WarnCapture(Logger logger) {
      super("IngestionSourceErrorClassifierTestCapture", null, null, true, Property.EMPTY_ARRAY);
      _logger = logger;
    }

    static WarnCapture attachTo(String loggerName) {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      WarnCapture capture = new WarnCapture(context.getLogger(loggerName));
      capture.start();
      capture._logger.addAppender(capture);
      return capture;
    }

    void detach() {
      _logger.removeAppender(this);
      stop();
      // Attaching registered a LoggerConfig for a logger log4j2.xml doesn't declare: remove it, since test classes
      // share this JVM
      LoggerContext context = _logger.getContext();
      context.getConfiguration().removeLogger(_logger.getName());
      context.updateLoggers();
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel() == Level.WARN) {
        _rendered.add(RENDERING.toSerializable(event));
      }
    }
  }

  @Test
  public void deeplyNestedSuppressedFailuresNeverOverflowTheStack()
      throws Exception {
    Throwable root = new RuntimeException("outermost");
    Throwable current = root;
    for (int i = 0; i < 5_000; i++) {
      Throwable nested = new RuntimeException("level " + i);
      current.addSuppressed(nested);
      current = nested;
    }
    current.addSuppressed(new ConnectException("deepest"));
    AtomicReference<Object> result = new AtomicReference<>();
    // A small stack, as on a busy consumer thread.
    Thread thread = new Thread(null, () -> {
      try {
        result.set(categoryOf(root));
      } catch (Throwable t) {
        result.set(t);
      }
    }, "classify", 256 * 1024);
    thread.start();
    thread.join();
    assertTrue(result.get() instanceof Optional, String.valueOf(result.get()));
  }

  @Test
  public void recordedEvidenceDecidesAndIsNeverAFailureOfItsOwn() {
    Throwable unreachable = IngestionSourceErrorEvidence.attach(new RuntimeException("connect failed"),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.UNREACHABLE));
    assertEquals(categoryOf(new RuntimeException("wrapper", unreachable)),
        Optional.of(IngestionSourceErrorCategory.SOURCE_READ));

    Throwable missingTopic = IngestionSourceErrorEvidence.attach(new RuntimeException("Topic does not exist"),
        new IngestionSourceErrorEvidence().setEvidence(0, IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION));
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

  private static <T extends Throwable> T sourceCall(T failure) {
    return IngestionSourceErrorEvidence.attach(failure, new IngestionSourceErrorEvidence().setBoundary(true));
  }

  @Test
  public void boundaryLimitsTheReadToTheSourcesBranches() {
    register(TEST_RULES);
    // A source read beside a refusal from storage that isn't the source, such as Pinot's own deep store
    Throwable failure = new RuntimeException("task failed");
    failure.addSuppressed(sourceCall(new RuntimeException("source read failed", new MissingException())));
    failure.addSuppressed(new RuntimeException("deep store read failed", new RefusedException(null)));
    assertEquals(categoryOf(failure), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));

    // A failure suppressed onto the source call (closing a stream on our side) doesn't inherit its boundary.
    Throwable sourceFailure = sourceCall(new RuntimeException("source call failed", new IllegalStateException()));
    sourceFailure.addSuppressed(new RuntimeException("close failed", new ConnectException("refused")));
    assertEquals(categoryOf(sourceFailure), Optional.empty());
    // Without a boundary anywhere, the caller vouches for the whole failure.
    Throwable unmarked = new RuntimeException("source call failed", new IllegalStateException());
    unmarked.addSuppressed(new RuntimeException("close failed", new ConnectException("refused")));
    assertEquals(categoryOf(unmarked), Optional.of(IngestionSourceErrorCategory.SOURCE_READ));
  }

  @Test
  public void classificationNamesTheDecidingExceptionAndItsEvidence() {
    register(TEST_RULES);
    RefusedException refusal = new RefusedException(null);
    IngestionSourceErrorEvidence evidence =
        new IngestionSourceErrorEvidence().setCredentialMode(IngestionSourceCredentialMode.STATIC);
    IngestionSourceErrorClassification classification = IngestionSourceErrorClassifier.classify(
        IngestionSourceErrorEvidence.attach(new RuntimeException("wrapper", refusal), evidence)).get();
    assertSame(classification.getException(), refusal);
    assertEquals(classification.getEvidence().getCredentialMode(), IngestionSourceCredentialMode.STATIC);
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
  public void evidenceDropsUserInfoAndQueryAndBoundsItsTokens() {
    IngestionSourceErrorEvidence evidence = new IngestionSourceErrorEvidence()
        .setResource("https://fixture-user:fixture-secret@registry.example.com:8081/subjects?token=fixture-token")
        .setEvidence(403, "denied\nforged log line" + "x".repeat(200))
        .setTarget("schema registry");
    assertEquals(evidence.getResource(), "https://registry.example.com:8081");
    assertEquals(evidence.getTarget(), "schema_registry");
    String code = evidence.getCode();
    assertTrue(code.length() <= 64, code);
    assertFalse(code.contains("\n"), code);
    assertEquals(new IngestionSourceErrorEvidence().setResource("broker-1:9092 SASL_SSL").getResource(), "unknown");
    // One replacement for each character, a supplementary one or a lone surrogate included, and 64 at most.
    assertEquals(IngestionSourceErrorVerdict.token("a😀b\uD83D c"), "a_b__c");
    assertEquals(IngestionSourceErrorVerdict.token("😀".repeat(100_000)), "_".repeat(64));

    Throwable failure = IngestionSourceErrorEvidence.attach(new RuntimeException(), evidence);
    String printed = IngestionSourceErrorEvidence.of(failure).getMessage();
    assertFalse(printed.contains("fixture-secret"), printed);
    assertFalse(printed.contains("fixture-token"), printed);
    assertFalse(printed.contains("fixture-user"), printed);
    assertNull(IngestionSourceErrorEvidence.of(new RuntimeException()));
  }
}
