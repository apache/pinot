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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.DeprecatedTableConfigValidationUtils.DeprecatedConfigRule;
import org.apache.pinot.common.utils.config.DeprecatedTableConfigValidationUtils.Result;
import org.apache.pinot.common.utils.config.DeprecatedTableConfigValidationUtils.Severity;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.DeprecatedConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class DeprecatedTableConfigValidationUtilsTest {

  @Test
  public void testReportsDeprecatedConfigsOnCreateAsWarnings()
      throws Exception {
    // Soft-launch policy: every parseable @DeprecatedConfig annotation is reported as a WARNING on create so
    // legacy callers (TableConfigBuilder setters, integration test bases, downstream automations) keep working.
    // A follow-up PR can promote to ERROR after the codebase migrates off these paths.
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"replicasPerPartition\":\"APPEND\",\"minimizeDataMovement\":false},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexType\":\"INVERTED\"}],"
        + "\"instanceAssignmentConfigMap\":{\"CONSUMING\":{\"replicaGroupPartitionConfig\":"
        + "{\"minimizeDataMovement\":false}}}}");

    List<String> warnings =
        DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, "realtime");
    assertTrue(warnings.stream().anyMatch(w -> w.contains("realtime.segmentsConfig.replicasPerPartition")), warnings
        .toString());
    assertTrue(warnings.stream().anyMatch(w -> w.contains("realtime.segmentsConfig.minimizeDataMovement")), warnings
        .toString());
    assertTrue(warnings.stream().anyMatch(w -> w.contains("realtime.fieldConfigList[0].indexType")), warnings
        .toString());
    assertTrue(warnings.stream().anyMatch(w -> w.contains(
            "realtime.instanceAssignmentConfigMap.CONSUMING.replicaGroupPartitionConfig.minimizeDataMovement")),
        warnings.toString());
  }

  @Test
  public void testCurrentVersionDeprecationIsWarningNotError()
      throws Exception {
    // tableIndexConfig.createInvertedIndexDuringSegmentGeneration is annotated with since=1.6.0, matching the
    // current Pinot release line. It is reported as a warning (matches the soft-launch policy where every
    // parseable `since` classifies as a warning).
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode(
        "{\"tableIndexConfig\":{\"createInvertedIndexDuringSegmentGeneration\":false}}");

    List<String> warnings = DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, null);
    assertTrue(warnings.stream().anyMatch(
            w -> w.contains("tableIndexConfig.createInvertedIndexDuringSegmentGeneration")),
        "expected warning for current-version deprecation, got: " + warnings);
  }

  @Test
  public void testAllowsModernConfigsOnCreate()
      throws Exception {
    JsonNode tableConfigJson = JsonUtils.stringToJsonNode("{"
        + "\"segmentsConfig\":{\"replication\":\"1\"},"
        + "\"fieldConfigList\":[{\"name\":\"c1\",\"indexTypes\":[\"INVERTED\"]}],"
        + "\"ingestionConfig\":{\"batchIngestionConfig\":{\"segmentIngestionType\":\"APPEND\"},"
        + "\"streamIngestionConfig\":{\"streamConfigMaps\":[{\"streamType\":\"kafka\"}]}}}");

    DeprecatedTableConfigValidationUtils.validateOnCreate(tableConfigJson, null);
  }

  @Test
  public void testUpdateAllowsUnchangedLegacyDeprecatedValue()
      throws Exception {
    // Legacy config is already stored with replicasPerPartition=APPEND. Re-submitting the same value must NOT trigger
    // an error: the diff sees the value as unchanged.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replicasPerPartition\":\"APPEND\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replicasPerPartition\":\"APPEND\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertFalse(result.hasErrors(), "errors=" + result.getErrors());
    assertFalse(result.hasWarnings(), "warnings=" + result.getWarnings());
  }

  @Test
  public void testUpdateReportsValueChangeOnDeprecatedFieldAsWarning()
      throws Exception {
    // Legacy config had replicasPerPartition=APPEND. The update changes it to REFRESH — the diff treats this as
    // a new write to a deprecated key and reports a warning under the soft-launch policy.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replicasPerPartition\":\"APPEND\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replicasPerPartition\":\"REFRESH\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings(), "expected warning on changed deprecated value");
    assertTrue(result.getWarnings().get(0).contains("segmentsConfig.replicasPerPartition"),
        result.getWarnings().toString());
  }

  @Test
  public void testUpdateAllowsReSubmittedDefaultValueWhenAbsentFromStored()
      throws Exception {
    // Many deprecated booleans carry @JsonInclude(NON_DEFAULT). A previous create with `enableSnapshot: false`
    // (the type default) is stripped at ZK write time, so the stored config has no `enableSnapshot` key. On PUT,
    // the diff sees the path as missing in the stored config but present in the new submission with the type
    // default — the validator must treat this as a no-op so users can re-submit cached configs unchanged.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":false}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertFalse(result.hasErrors(), "errors=" + result.getErrors());
    assertFalse(result.hasWarnings(), "warnings=" + result.getWarnings());
  }

  @Test
  public void testUpdateAllowsLegacyIndexTypeWhenStoredAsSingletonIndexTypes()
      throws Exception {
    // Even when stored configs were written before `@DeprecatedConfig` shipped (so the legacy `indexType` key is
    // absent and only the singleton `indexTypes` array survives in ZK), re-submitting the same legacy payload on
    // update must remain idempotent. The validator's `isModernFieldConfigIndexTypeEquivalent` shortcut handles
    // this old-stored / new-input shape mismatch.
    JsonNode oldJson =
        JsonUtils.stringToJsonNode("{\"fieldConfigList\":[{\"name\":\"c1\",\"indexTypes\":[\"INVERTED\"]}]}");
    JsonNode newJson =
        JsonUtils.stringToJsonNode("{\"fieldConfigList\":[{\"name\":\"c1\",\"indexType\":\"INVERTED\"}]}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertFalse(result.hasErrors(), "errors=" + result.getErrors());
    assertFalse(result.hasWarnings(), "warnings=" + result.getWarnings());
  }

  @Test
  public void testUpdateReportsLegacyIndexTypeWhenStoredIndexTypesAreNotEquivalent()
      throws Exception {
    // The compatibility shortcut only applies to a singleton `indexTypes` value that exactly matches `indexType`.
    // Multi-index stored configs cannot be represented by legacy `indexType`, so the deprecated write is reported.
    JsonNode oldJson = JsonUtils.stringToJsonNode(
        "{\"fieldConfigList\":[{\"name\":\"c1\",\"indexTypes\":[\"INVERTED\",\"RANGE\"]}]}");
    JsonNode newJson =
        JsonUtils.stringToJsonNode("{\"fieldConfigList\":[{\"name\":\"c1\",\"indexType\":\"INVERTED\"}]}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings(), "expected warning on non-equivalent legacy indexType");
    assertTrue(result.getWarnings().get(0).contains("fieldConfigList[0].indexType"),
        result.getWarnings().toString());
  }

  @Test
  public void testUpdateTreatsExplicitJsonNullStoredValueAsPresent()
      throws Exception {
    // The stored JSON has an explicit `null` value for a deprecated path. The new submission flips it to a
    // non-default `true`. Both differ, so the diff must report it (the default-skip applies only when the path
    // was *absent* in the stored config, not when it was present-but-null).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":null}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":true}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings(), "expected warning on flip from null to true");
    assertTrue(result.getWarnings().get(0).contains("upsertConfig.enableSnapshot"),
        result.getWarnings().toString());
  }

  @Test
  public void testUpdateReportsDeliberateFlipFromNonDefaultToDefaultAsWarning()
      throws Exception {
    // The default-skip optimisation must not silently swallow a deliberate value flip on an existing field.
    // Stored config has `enableSnapshot: true`; user submits `enableSnapshot: false` — this is a value change on
    // a deprecated path and is reported as a warning under the soft-launch policy.
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":true}}");
    JsonNode newJson = JsonUtils.stringToJsonNode("{\"upsertConfig\":{\"enableSnapshot\":false}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings(), "expected warning on deliberate flip true → false");
    assertTrue(result.getWarnings().get(0).contains("upsertConfig.enableSnapshot"),
        result.getWarnings().toString());
  }

  @Test
  public void testUpdateReportsEmptyStringValueForNullDefaultField()
      throws Exception {
    // String-returning deprecated getters initialise to null (the Java default), not "". A user submitting
    // "replicasPerPartition":"" on update — when the stored config lacks the key — is supplying a real value that
    // Jackson would NOT elide under NON_DEFAULT, and is flagged as a warning. Locks the textual branch of
    // isJacksonDefault returning false (rather than treating empty string as default).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replication\":\"1\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode(
        "{\"segmentsConfig\":{\"replication\":\"1\",\"replicasPerPartition\":\"\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings(), "expected warning on empty-string value for deprecated path");
    assertTrue(result.getWarnings().get(0).contains("segmentsConfig.replicasPerPartition"),
        result.getWarnings().toString());
  }

  @Test
  public void testUpdateReportsNewlyIntroducedDeprecatedField()
      throws Exception {
    // Legacy config did not contain the deprecated field. Adding it on update fires a warning under the
    // soft-launch policy (parseable since classifies as warning).
    JsonNode oldJson = JsonUtils.stringToJsonNode("{\"segmentsConfig\":{\"replication\":\"1\"}}");
    JsonNode newJson = JsonUtils.stringToJsonNode(
        "{\"segmentsConfig\":{\"replication\":\"1\",\"replicasPerPartition\":\"APPEND\"}}");

    Result result = DeprecatedTableConfigValidationUtils.validate(newJson, oldJson, null);
    assertTrue(result.hasWarnings());
    assertTrue(result.getWarnings().get(0).contains("segmentsConfig.replicasPerPartition"));
  }

  @Test
  public void testMajorMinorParsing() {
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6.0"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6.0-SNAPSHOT"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("1.6"), "1.6");
    assertEquals(DeprecatedTableConfigValidationUtils.majorMinor("12.345.6"), "12.345");
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor(null));
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor("garbage"));
    assertNull(DeprecatedTableConfigValidationUtils.majorMinor("1"));
  }

  @Test
  public void testSeverityClassification() {
    // An unparseable annotation `since` reflects a code-side bug and classifies as ERROR.
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage"), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity(""), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1"), Severity.ERROR);
  }

  @Test
  public void testSeverityIsWarningForAllParseableSinceUnderSoftLaunch() {
    // Soft-launch policy: every parseable `since` returns WARNING, regardless of the running Pinot version, so
    // existing callers that already use deprecated keys keep working. ERROR is reserved for unparseable values
    // (which reflect a code-side annotation bug, not user-supplied data).
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.6.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.5.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("0.3.0", null), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.6.0", "1.6"), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("1.5.0", "1.6"), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("0.3.0", "1.6"), Severity.WARNING);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage", null), Severity.ERROR);
    assertEquals(DeprecatedTableConfigValidationUtils.classifySeverity("garbage", "1.6"), Severity.ERROR);
  }

  /// Locks in the contract assumed by [DeprecatedTableConfigValidationUtils#isJacksonDefault]: every getter
  /// annotated with [DeprecatedConfig @DeprecatedConfig] must be on a bean whose
  /// no-arg-constructed default value for that getter is a Java zero-value (`false`, `0`, `null`, empty string,
  /// empty collection). If a future contributor adds the annotation on a getter whose bean default is non-zero
  /// (e.g. an int defaulting to `1`), this test fails at compile/test time rather than silently suppressing
  /// deprecation diffs at runtime.
  @Test
  public void testEveryAnnotatedGetterHasJavaZeroDefault()
      throws Exception {
    for (DeprecatedConfigRule rule : DeprecatedTableConfigValidationUtils.rulesForTesting()) {
      Class<?> owner = rule.leafOwner();
      Method getter = rule.leafGetter();
      Object defaultInstance = instantiateForDefaultLookup(owner);
      Object value = getter.invoke(defaultInstance);
      JsonNode jsonValue = JsonUtils.objectToJsonNode(value);
      assertTrue(DeprecatedTableConfigValidationUtils.isJacksonDefault(jsonValue),
          "Java default for " + owner.getSimpleName() + "#" + getter.getName() + " (path=" + rule.pathSegments()
              + ") is " + value + " (JSON=" + jsonValue + "), which isJacksonDefault treats as non-default. "
              + "Either change the field's Java default to the type-zero value, OR teach isJacksonDefault to "
              + "consult the bean's actual default via reflection.");
    }
  }

  /// Beans whose declared constructor reflection-with-placeholder-args is known to produce the same Java field
  /// defaults that Jackson would observe. New beans that hit the third fallback must be added here explicitly
  /// (after a human confirms the reflective ctor preserves the bean's default field state) — otherwise the
  /// test fails loud, so a future `@DeprecatedConfig` on a non-zero-default getter cannot silently slip past.
  private static final Set<String> ALLOW_REFLECTIVE_CTOR_FALLBACK = Set.of(
      "org.apache.pinot.spi.config.table.FieldConfig",
      "org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig");

  /// Returns a default-state instance of `owner` suitable for reading the Java zero-value of each declared field.
  /// Strategy: (1) prefer a no-arg constructor; (2) fall back to Jackson deserialization of `"{}"`; (3) for an
  /// allowlisted bean (see [#ALLOW_REFLECTIVE_CTOR_FALLBACK]), invoke the smallest declared constructor with
  /// `null` for reference params and zero/placeholder for primitives. Falling through to (3) for an unlisted
  /// bean throws so a new `@DeprecatedConfig` on a non-zero-default getter cannot silently skip the contract.
  private static Object instantiateForDefaultLookup(Class<?> owner)
      throws Exception {
    try {
      return owner.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException ignored) {
      // No no-arg constructor; fall through.
    }
    try {
      return JsonUtils.stringToObject("{}", owner);
    } catch (Exception ignored) {
      // @JsonCreator requires fields that aren't in "{}". Try reflective construction with null/zero defaults.
    }
    if (!ALLOW_REFLECTIVE_CTOR_FALLBACK.contains(owner.getName())) {
      throw new AssertionError("Bean " + owner.getName() + " requires reflective-ctor fallback to instantiate "
          + "for @DeprecatedConfig default-value lookup, but is not in ALLOW_REFLECTIVE_CTOR_FALLBACK. "
          + "Confirm by hand that the bean's reflective-zero-arg ctor produces the same field defaults that "
          + "Jackson would deserialize, then add the FQN to the allowlist.");
    }
    Constructor<?>[] ctors = owner.getDeclaredConstructors();
    // Sort by parameter count ascending — prefer the smallest viable constructor.
    Arrays.sort(ctors, Comparator.comparingInt(Constructor::getParameterCount));
    Exception lastFailure = null;
    for (Constructor<?> ctor : ctors) {
      Object[] args = new Object[ctor.getParameterCount()];
      Class<?>[] paramTypes = ctor.getParameterTypes();
      for (int i = 0; i < args.length; i++) {
        args[i] = primitiveZeroOrPlaceholder(paramTypes[i]);
      }
      try {
        ctor.setAccessible(true);
        return ctor.newInstance(args);
      } catch (Exception e) {
        lastFailure = e;
      }
    }
    throw new AssertionError("Cannot instantiate " + owner.getName() + " for default-value lookup", lastFailure);
  }

  @Nullable
  private static Object primitiveZeroOrPlaceholder(Class<?> type) {
    // String params may carry Preconditions.checkArgument(name != null) — supply the empty string rather than null
    // or a non-default sentinel. Using "" means the test fails only when a real String getter has a non-default
    // default; a "_test"-like sentinel would surface as a confusing "got `_test`, expected default" failure for
    // any future String getter even when the bean's real default is also non-empty.
    if (type == String.class) {
      return "";
    }
    if (!type.isPrimitive()) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == char.class) {
      return '\0';
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == float.class) {
      return 0f;
    }
    if (type == double.class) {
      return 0d;
    }
    throw new IllegalStateException("Unknown primitive type: " + type);
  }

  /// Locks the rule-list cache. The discovery walk is intentionally one-shot at class-init (lazy holder
  /// pattern); if a future refactor accidentally re-runs `discoverRules()` on every `validate(...)` call, this
  /// test fails fast. We check that the underlying backing list is the same JVM-identity instance, not just
  /// equal — the unmodifiable wrapper returned by `rulesForTesting()` rewraps each call, so we compare via
  /// the rule pointers it contains instead.
  @Test
  public void testRulesListIsCachedAcrossCalls() {
    List<DeprecatedConfigRule> first = DeprecatedTableConfigValidationUtils.rulesForTesting();
    List<DeprecatedConfigRule> second = DeprecatedTableConfigValidationUtils.rulesForTesting();
    assertEquals(first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      assertTrue(first.get(i) == second.get(i),
          "rulesForTesting() should return cached rule instances; rule " + i + " differs across calls. "
              + "Check that Rules.ALL is still a static final and that no refactor moved discoverRules() to a "
              + "per-request path.");
    }
  }

  /// Eagerly forces the static `Rules.ALL` discovery walk at test time so a duplicate JsonProperty annotation
  /// or other discovery-walk bug surfaces as a JUnit failure instead of as an `ExceptionInInitializerError`
  /// that takes down every controller endpoint after deployment. Today, `rulesForTesting()` is the only way to
  /// trigger the lazy-holder load, so any test class that touches the validator already serves this purpose;
  /// this test makes the intent explicit and adds a friendly failure message pointing at the likely cause.
  @Test
  public void testDiscoveryWalkDoesNotThrowAtClassLoad() {
    try {
      DeprecatedTableConfigValidationUtils.rulesForTesting();
    } catch (Throwable t) {
      throw new AssertionError(
          "DeprecatedTableConfigValidationUtils discovery walk failed at class load. The lazy holder (Rules.ALL) "
              + "throws ExceptionInInitializerError on every subsequent reference, so a controller deploying with "
              + "this build would 500 on every table CREATE/UPDATE/VALIDATE/TUNE/COPY endpoint. Likely causes: a "
              + "duplicate JsonProperty name across two @DeprecatedConfig getters on the same bean, an annotation "
              + "with an unparseable since=\"...\" value, or a nested bean cycle exceeding MAX_WALK_DEPTH. Root "
              + "cause:",
          t);
    }
  }

  /// Locks the soft-launch flag at its initial value so the flip cannot land without an intentional, reviewable
  /// edit. The PR that flips this constant MUST first land the items enumerated in [#SOFT_LAUNCH_WARNING_ONLY]'s
  /// Javadoc — version-checked CAS on the update path, a concurrency regression test, an injected-version test
  /// seam, and an integration test of the ERROR-path. Once those are in place, edit BOTH `SOFT_LAUNCH_WARNING_ONLY`
  /// and this test in the same commit so the test continues to lock the post-flip value.
  ///
  /// Promotion pre-conditions coverage map (cross-references for future maintainers):
  /// 1. Version-checked CAS — implemented in `PinotHelixResourceManager.setExistingTableConfig(...)`. Threaded
  ///    through `PinotTableRestletResource.updateTableConfig` and `TableConfigsRestletResource.updateConfig`.
  /// 2. Concurrency regression test —
  ///    `PinotHelixResourceManagerStatelessTest#testUpdateTableConfigCasConflictThrowsConflictException`.
  ///    Exercises the T1-read / T2-write / T1-CAS-fail interleaving end-to-end.
  /// 3. Version-injection seam — see `classifySeverity(String, String)` overload in
  ///    `DeprecatedTableConfigValidationUtils`; tests pass an injected `currentMajorMinor` to exercise the
  ///    older-than-current → ERROR branch.
  /// 4. ERROR-path REST integration test —
  ///    `PinotTableRestletResourceTest#testErrorSeverityRuleRejectsCreateAs400`. Uses the `@VisibleForTesting`
  ///    rule-override seam to inject a synthetic ERROR rule and asserts 400.
  @Test
  public void testSoftLaunchFlagDefaultsToWarningOnly() {
    assertTrue(DeprecatedTableConfigValidationUtils.SOFT_LAUNCH_WARNING_ONLY,
        "SOFT_LAUNCH_WARNING_ONLY must remain true until the four promotion pre-conditions in its Javadoc are met. "
            + "Edit this test in the same commit that flips the flag, and ensure the promotion PR also adds: "
            + "(1) version-checked CAS on PinotTableRestletResource and TableConfigsRestletResource update paths, "
            + "(2) a concurrency regression test that exercises the read→write window, "
            + "(3) a test seam for the older-than-current → ERROR branch in classifySeverity, "
            + "(4) an integration test that exercises the validator's ERROR-path end-to-end through the REST stack.");
  }

  /// Locks the build-time invariant that every `@DeprecatedConfig.since` value is parseable. Without this guard
  /// a typo like `@DeprecatedConfig(since = "")` would classify as ERROR (Severity.ERROR is the unparseable-
  /// since fallback in `classifySeverity`). Today, under `SOFT_LAUNCH_WARNING_ONLY = true`, an ERROR fires the
  /// throw branch in `validateOnCreate`/`validateOnUpdate` → 400 from the REST endpoints. When the soft-launch
  /// flag flips, a since-typo on any annotation becomes a permanent production outage for every PUT/POST that
  /// touches that path. Fail at class-load instead.
  @Test
  public void testEveryRuleHasParseableSince() {
    for (DeprecatedConfigRule rule : DeprecatedTableConfigValidationUtils.rulesForTesting()) {
      assertEquals(rule.severity(), Severity.WARNING,
          "Rule at " + rule.pathSegments() + " has @DeprecatedConfig(since=\"" + rule.since() + "\") that the "
              + "soft-launch classifier resolved to " + rule.severity() + ". Under SOFT_LAUNCH_WARNING_ONLY=true "
              + "this means `since` is unparseable. Use MAJOR.MINOR or MAJOR.MINOR.PATCH form (e.g. \"1.6.0\").");
    }
  }

  /// Locks the dual-annotation invariant: every getter annotated with `@DeprecatedConfig` (the validator metadata)
  /// must also carry `@Deprecated` (the JDK marker that surfaces IDE/compiler warnings). Without this guard a
  /// future contributor can add a `@DeprecatedConfig` annotation without `@Deprecated`, so callers continue to
  /// use the field without the IDE warning that should signal a migration.
  @Test
  public void testEveryAnnotatedGetterAlsoCarriesJdkDeprecated() {
    for (DeprecatedConfigRule rule : DeprecatedTableConfigValidationUtils.rulesForTesting()) {
      Method getter = rule.leafGetter();
      assertTrue(getter.isAnnotationPresent(Deprecated.class),
          "Getter " + rule.leafOwner().getSimpleName() + "#" + getter.getName()
              + " has @DeprecatedConfig but lacks @Deprecated. Both annotations are required: @DeprecatedConfig"
              + " for the runtime validator, @Deprecated for the IDE/compiler warning.");
    }
  }

  /// Locks the BaseJsonConfig invariant relied on by [DeprecatedTableConfigValidationUtils#isConfigBean]. The
  /// reflective discovery walk only descends into classes that extend `BaseJsonConfig`. If a future SPI bean
  /// reachable from `TableConfig` (via a getter return type, a Map value type, or a Collection element type)
  /// were to NOT extend `BaseJsonConfig`, any `@DeprecatedConfig` annotation on its getters would be silently
  /// invisible to the validator. This test re-walks the TableConfig graph and asserts every nested bean type
  /// is a `BaseJsonConfig` descendant — failing loudly the moment the invariant breaks.
  @Test
  public void testEveryNestedConfigBeanExtendsBaseJsonConfig() {
    Set<Class<?>> visited = new HashSet<>();
    List<String> offenders = new ArrayList<>();
    walkConfigBeans(TableConfig.class, visited, offenders);
    assertTrue(offenders.isEmpty(),
        "Found getter return types reachable from TableConfig whose declaring class is NOT a BaseJsonConfig "
            + "descendant. The validator's reflective walk will silently skip @DeprecatedConfig annotations on "
            + "these classes. Offenders:\n  " + String.join("\n  ", offenders));
  }

  private static void walkConfigBeans(Class<?> type, Set<Class<?>> visited, List<String> out) {
    if (type == null || type.isPrimitive() || type == Object.class || !visited.add(type)) {
      return;
    }
    if (type.getName().startsWith("java.") || type.getName().startsWith("javax.")
        || type.getName().startsWith("com.fasterxml.jackson.")) {
      return;
    }
    for (Method method : type.getMethods()) {
      if (method.getParameterCount() != 0 || method.getDeclaringClass() == Object.class
          || Modifier.isStatic(method.getModifiers())
          || method.getReturnType() == void.class) {
        continue;
      }
      String name = method.getName();
      if (!name.startsWith("get") && !(name.startsWith("is") && (method.getReturnType() == boolean.class
          || method.getReturnType() == Boolean.class))) {
        continue;
      }
      Class<?> ret = method.getReturnType();
      if (BaseJsonConfig.class.isAssignableFrom(ret)
          && ret != BaseJsonConfig.class) {
        walkConfigBeans(ret, visited, out);
      } else if (ret.getPackageName().startsWith("org.apache.pinot")
          && !BaseJsonConfig.class.isAssignableFrom(ret)
          && !ret.isEnum() && !ret.isInterface() && !ret.isAnnotation() && !ret.isPrimitive()
          && method.getAnnotation(DeprecatedConfig.class) != null) {
        // The getter is annotated with @DeprecatedConfig but its return type is a Pinot type that is NOT a
        // BaseJsonConfig descendant — this is the silent-skip risk the test guards against.
        out.add(type.getSimpleName() + "#" + name + "() returns " + ret.getName()
            + " (not a BaseJsonConfig descendant)");
      }
    }
  }

  @Test
  public void testRulesDiscoveredFromAnnotations() {
    // Sanity check that the annotation walk picks up the expected paths from TableConfig.
    boolean foundIndexType = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of("fieldConfigList", "*", "indexType")));
    assertTrue(foundIndexType, "expected fieldConfigList[*].indexType rule");

    boolean foundNestedMinimize = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of(
            "instanceAssignmentConfigMap", "*", "replicaGroupPartitionConfig", "minimizeDataMovement")));
    assertTrue(foundNestedMinimize, "expected nested map-wildcard rule");

    boolean foundStreamConfigs = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of("tableIndexConfig", "streamConfigs")));
    assertTrue(foundStreamConfigs, "expected tableIndexConfig.streamConfigs rule");

    boolean foundSegmentPushType = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of("segmentsConfig", "segmentPushType")));
    assertTrue(foundSegmentPushType, "expected segmentsConfig.segmentPushType rule");

    boolean foundSegmentPushFrequency = DeprecatedTableConfigValidationUtils.rulesForTesting().stream()
        .anyMatch(rule -> rule.pathSegments().equals(List.of("segmentsConfig", "segmentPushFrequency")));
    assertTrue(foundSegmentPushFrequency, "expected segmentsConfig.segmentPushFrequency rule");
  }

  /// Provides every rule discovered by the annotation walk so the parameterized test below covers the full set
  /// 1:1. If a new {@link DeprecatedConfig @DeprecatedConfig} is added on a getter, this
  /// test automatically exercises it without needing a new test case.
  @DataProvider(name = "allRules")
  public Object[][] allRules() {
    List<DeprecatedConfigRule> rules = DeprecatedTableConfigValidationUtils.rulesForTesting();
    Object[][] data = new Object[rules.size()][];
    for (int i = 0; i < rules.size(); i++) {
      data[i] = new Object[] {rules.get(i)};
    }
    return data;
  }

  @Test(dataProvider = "allRules")
  public void testEveryRuleFiresOnSyntheticInputAsArrayWildcard(DeprecatedConfigRule rule)
      throws Exception {
    runEveryRuleCase(rule, /* arrayWildcard */ true);
  }

  @Test(dataProvider = "allRules")
  public void testEveryRuleFiresOnSyntheticInputAsMapWildcard(DeprecatedConfigRule rule)
      throws Exception {
    runEveryRuleCase(rule, /* arrayWildcard */ false);
  }

  /// Builds a JSON tree containing the deprecated path and asserts the rule fires. The {@code arrayWildcard} flag
  /// controls how `*` segments are realised: as `[ {...} ]` (array branch) or `{"x":...}` (object branch). Running
  /// both shapes ensures `collectMatches` is exercised on both wildcard branches for every rule.
  private static void runEveryRuleCase(DeprecatedConfigRule rule, boolean arrayWildcard)
      throws Exception {
    String synthetic = synthesizeJsonForPath(rule.pathSegments(), arrayWildcard);
    String expectedPath = expectedPathInMessage(rule.pathSegments(), arrayWildcard);

    Result result = DeprecatedTableConfigValidationUtils.validate(JsonUtils.stringToJsonNode(synthetic), null, null);
    if (rule.severity() == Severity.ERROR) {
      assertTrue(result.getErrors().stream().anyMatch(m -> m.contains(expectedPath)),
          "expected error containing '" + expectedPath + "', got errors=" + result.getErrors() + ", warnings="
              + result.getWarnings());
    } else {
      assertTrue(result.getWarnings().stream().anyMatch(m -> m.contains(expectedPath)),
          "expected warning containing '" + expectedPath + "', got warnings=" + result.getWarnings() + ", errors="
              + result.getErrors());
    }
  }

  private static String synthesizeJsonForPath(List<String> path, boolean arrayWildcard) {
    StringBuilder open = new StringBuilder();
    StringBuilder close = new StringBuilder();
    for (String segment : path.subList(0, path.size() - 1)) {
      if ("*".equals(segment)) {
        if (arrayWildcard) {
          open.append("[");
          close.insert(0, "]");
        } else {
          open.append("{\"x\":");
          close.insert(0, "}");
        }
      } else {
        open.append("{\"").append(segment).append("\":");
        close.insert(0, "}");
      }
    }
    String leaf = path.get(path.size() - 1);
    if ("*".equals(leaf)) {
      open.append(arrayWildcard ? "[\"v\"]" : "{\"x\":\"v\"}");
    } else {
      open.append("{\"").append(leaf).append("\":\"v\"}");
    }
    return open.append(close).toString();
  }

  private static String expectedPathInMessage(List<String> path, boolean arrayWildcard) {
    StringBuilder sb = new StringBuilder();
    for (String segment : path) {
      if ("*".equals(segment) && arrayWildcard) {
        // The walker emits `[<index>]` (no preceding `.`) for array entries.
        sb.append("[0]");
      } else {
        String key = "*".equals(segment) ? "x" : segment;
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(key);
      }
    }
    return sb.toString();
  }
}
