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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.DeprecatedConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Validates raw table-config JSON for deprecated properties on create and update paths.
///
/// Rules are derived at class-load time from [DeprecatedConfig] annotations on getters reachable from [TableConfig].
/// The current soft-launch policy reports every parseable annotation `since` value as [Severity#WARNING] so existing
/// callers continue to work while seeing migration guidance. Only an unparseable annotation `since` value classifies
/// as [Severity#ERROR], since that case is a code-side bug rather than user-supplied data.
///
/// The validator operates on raw JSON rather than the deserialized [TableConfig] so it can detect explicitly
/// provided deprecated keys even when they carry default or false-y values that Jackson would otherwise elide.
public final class DeprecatedTableConfigValidationUtils {
  private DeprecatedTableConfigValidationUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DeprecatedTableConfigValidationUtils.class);
  private static final String WILDCARD = "*";
  private static final String FIELD_CONFIG_INDEX_TYPES_KEY = "indexTypes";
  private static final List<String> FIELD_CONFIG_INDEX_TYPE_PATH =
      List.of(TableConfig.FIELD_CONFIG_LIST_KEY, WILDCARD, "indexType");
  // Lazy holder. The list of rules is computed exactly once on first reference; classifySeverity calls
  // currentMajorMinor() directly so there is no source-order dependency between two interdependent constants.
  private static final class Rules {
    private static final List<DeprecatedConfigRule> ALL = discoverRules();
  }

  public enum Severity {
    WARNING, ERROR
  }

  /// Result of validating a table-config JSON, exposing errors and warnings separately so callers can decide
  /// whether to reject the request or surface non-fatal warnings to the user.
  public static final class Result {
    private final List<String> _errors;
    private final List<String> _warnings;

    private Result(List<String> errors, List<String> warnings) {
      _errors = List.copyOf(errors);
      _warnings = List.copyOf(warnings);
    }

    public List<String> getErrors() {
      return _errors;
    }

    public List<String> getWarnings() {
      return _warnings;
    }

    public boolean hasErrors() {
      return !_errors.isEmpty();
    }

    public boolean hasWarnings() {
      return !_warnings.isEmpty();
    }
  }

  /// Validates a table-config JSON document. When `oldTableConfigJson` is non-null, only paths whose value newly
  /// appears in `newTableConfigJson` or whose value differs from the corresponding path in `oldTableConfigJson` are
  /// reported, so re-submitting an unchanged legacy field on an update is a no-op. When `oldTableConfigJson` is null
  /// (creation path), every present deprecated path is reported.
  ///
  /// The `rootPathPrefix` argument controls the path string surfaced in deprecation warnings. It must mirror the
  /// shape of the user-submitted JSON so the emitted path can be located back in the request body:
  /// - `/tables/...` endpoints submit a single TableConfig whose root IS the bean, so pass `null` (no prefix).
  /// - `/tableConfigs/...` endpoints submit `{"offline": {...}, "realtime": {...}}`, so pass `"offline"` /
  ///   `"realtime"` when iterating each sub-section so warnings read e.g.
  ///   `realtime.segmentsConfig.replicasPerPartition`.
  ///
  /// @param newTableConfigJson the incoming table config to validate; must not be `null`
  /// @param oldTableConfigJson the currently-stored table config to diff against, or `null` for create paths
  /// @param rootPathPrefix optional prefix for emitted paths, matching the user-submitted JSON structure
  public static Result validate(JsonNode newTableConfigJson, @Nullable JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix) {
    return validateWithRules(newTableConfigJson, oldTableConfigJson, rootPathPrefix, Rules.ALL);
  }

  /// Test seam for [#validate]. Allows unit tests to drive a synthetic rule list end-to-end, e.g. to verify
  /// that an ERROR-severity rule trips [#validateOnCreate]'s throw branch without needing to flip the
  /// [#SOFT_LAUNCH_WARNING_ONLY] feature gate.
  static Result validateWithRules(JsonNode newTableConfigJson, @Nullable JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix, List<DeprecatedConfigRule> rules) {
    Objects.requireNonNull(newTableConfigJson, "newTableConfigJson");
    List<String> errors = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    String prefix = rootPathPrefix == null ? "" : rootPathPrefix;
    for (DeprecatedConfigRule rule : rules) {
      rule.collect(newTableConfigJson, oldTableConfigJson, prefix, errors, warnings);
    }
    return new Result(errors, warnings);
  }

  /// Test seam exposing the throw-on-errors decision branch in [#validateOnCreate]. Tests assemble a
  /// synthetic [Result] (e.g. via [#validateWithRules] with a single ERROR-severity rule) and pass it here.
  /// Production callers should always invoke [#validateOnCreate] which constructs the Result via [#validate].
  static List<String> throwIfErrorsOnCreate(Result result) {
    if (result.hasErrors()) {
      throw new IllegalArgumentException("Deprecated table config properties are not allowed on table creation: "
          + String.join("; ", result.getErrors()));
    }
    if (result.hasWarnings()) {
      LOGGER.warn("Deprecated table config properties on creation: {}", String.join("; ", result.getWarnings()));
    }
    return result.getWarnings();
  }

  /// Validates a freshly-submitted table config (no prior stored value). On any error the method throws
  /// [IllegalArgumentException]; warnings are returned so the caller can surface them in the response.
  public static List<String> validateOnCreate(JsonNode newTableConfigJson, @Nullable String rootPathPrefix) {
    return throwIfErrorsOnCreate(validate(newTableConfigJson, null, rootPathPrefix));
  }

  /// Validates an updated table config against its currently-stored counterpart. Only newly-introduced or
  /// value-changed deprecated paths are reported, so legacy values that were already present do not block updates.
  /// On any error the method throws [IllegalArgumentException]; warnings are returned for the caller to surface.
  ///
  /// Callers must ensure the table exists before invoking this method; pass the stored config JSON (never `null`)
  /// for `oldTableConfigJson`. For paths where no stored counterpart exists, use [#validateOnCreate] instead.
  public static List<String> validateOnUpdate(JsonNode newTableConfigJson, JsonNode oldTableConfigJson,
      @Nullable String rootPathPrefix) {
    Objects.requireNonNull(oldTableConfigJson, "oldTableConfigJson; use validateOnCreate for create paths");
    Result result = validate(newTableConfigJson, oldTableConfigJson, rootPathPrefix);
    if (result.hasErrors()) {
      throw new IllegalArgumentException("Newly introduced deprecated table config properties are not allowed: "
          + String.join("; ", result.getErrors()));
    }
    if (result.hasWarnings()) {
      LOGGER.warn("Newly introduced deprecated table config properties on update: {}",
          String.join("; ", result.getWarnings()));
    }
    return result.getWarnings();
  }

  static List<DeprecatedConfigRule> rulesForTesting() {
    return Collections.unmodifiableList(Rules.ALL);
  }

  private static List<DeprecatedConfigRule> discoverRules() {
    List<DeprecatedConfigRule> rules = new ArrayList<>();
    walk(TableConfig.class, new ArrayList<>(), new HashSet<>(), rules);
    return Collections.unmodifiableList(rules);
  }

  // Safety cap on path depth to fail loudly if a future SPI change introduces a cycle that the visiting set
  // alone can't break (e.g. a Map<String, NestedBean> recursion through wildcards).
  private static final int MAX_WALK_DEPTH = 32;

  private static void walk(Type type, List<String> currentPath, Set<Class<?>> visiting,
      List<DeprecatedConfigRule> rules) {
    Preconditions.checkState(currentPath.size() < MAX_WALK_DEPTH,
        "Pathological depth in @DeprecatedConfig discovery walk (path=%s); check for a cycle in TableConfig nested "
            + "beans", currentPath);
    Class<?> rawClass = rawClass(type);
    if (rawClass == null) {
      return;
    }
    if (Map.class.isAssignableFrom(rawClass)) {
      Type valueType = typeArg(type, 1);
      if (valueType != null) {
        currentPath.add(WILDCARD);
        walk(valueType, currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
      return;
    }
    if (Collection.class.isAssignableFrom(rawClass)) {
      Type elemType = typeArg(type, 0);
      if (elemType != null) {
        currentPath.add(WILDCARD);
        walk(elemType, currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
      return;
    }
    if (rawClass.isArray()) {
      currentPath.add(WILDCARD);
      walk(rawClass.getComponentType(), currentPath, visiting, rules);
      currentPath.remove(currentPath.size() - 1);
      return;
    }
    if (!isConfigBean(rawClass) || !visiting.add(rawClass)) {
      return;
    }
    // Detect duplicate JSON property names on the same bean — e.g. both a `@JsonProperty("foo")` getter and a
    // bare `getFoo()`. `Class.getMethods()` returns methods in unspecified order, so a collision would let
    // either rule win non-deterministically and silently disable validation for the loser. Fail loud at
    // class-load time instead.
    Set<String> seenProperties = new HashSet<>();
    try {
      for (Method method : rawClass.getMethods()) {
        if (!isJsonAccessor(method)) {
          continue;
        }
        String propertyName = jsonPropertyName(method);
        if (propertyName == null) {
          continue;
        }
        Preconditions.checkState(seenProperties.add(propertyName),
            "Duplicate JSON property name '%s' on bean %s — both a @JsonProperty-annotated getter and a bare "
                + "getter map to the same name. Pick one or rename the @JsonProperty value so the deprecation "
                + "discovery walk is deterministic.", propertyName, rawClass.getName());
        currentPath.add(propertyName);
        DeprecatedConfig anno = method.getAnnotation(DeprecatedConfig.class);
        if (anno != null) {
          rules.add(new DeprecatedConfigRule(List.copyOf(currentPath), anno.replacement(), anno.since(),
              classifySeverity(anno.since()), rawClass, method));
        }
        walk(method.getGenericReturnType(), currentPath, visiting, rules);
        currentPath.remove(currentPath.size() - 1);
      }
    } finally {
      visiting.remove(rawClass);
    }
  }

  private static boolean isConfigBean(Class<?> rawClass) {
    return BaseJsonConfig.class.isAssignableFrom(rawClass) && rawClass != BaseJsonConfig.class;
  }

  private static boolean isJsonAccessor(Method method) {
    if (method.getParameterCount() != 0 || method.getDeclaringClass() == Object.class
        || Modifier.isStatic(method.getModifiers())) {
      return false;
    }
    Class<?> returnType = method.getReturnType();
    if (returnType == void.class) {
      return false;
    }
    String name = method.getName();
    if (name.startsWith("get") && name.length() > 3) {
      return true;
    }
    return name.startsWith("is") && name.length() > 2 && (returnType == boolean.class || returnType == Boolean.class);
  }

  @Nullable
  private static String jsonPropertyName(Method method) {
    JsonProperty jsonProperty = method.getAnnotation(JsonProperty.class);
    if (jsonProperty != null && !jsonProperty.value().isEmpty()) {
      return jsonProperty.value();
    }
    String name = method.getName();
    String stripped;
    if (name.startsWith("get")) {
      stripped = name.substring(3);
    } else if (name.startsWith("is")) {
      stripped = name.substring(2);
    } else {
      return null;
    }
    if (stripped.isEmpty()) {
      return null;
    }
    return Character.toLowerCase(stripped.charAt(0)) + stripped.substring(1);
  }

  @Nullable
  private static Class<?> rawClass(Type type) {
    if (type instanceof Class<?>) {
      return (Class<?>) type;
    }
    if (type instanceof ParameterizedType) {
      Type rawType = ((ParameterizedType) type).getRawType();
      if (rawType instanceof Class<?>) {
        return (Class<?>) rawType;
      }
    }
    return null;
  }

  @Nullable
  private static Type typeArg(Type type, int index) {
    if (type instanceof ParameterizedType) {
      Type[] args = ((ParameterizedType) type).getActualTypeArguments();
      if (index < args.length) {
        return args[index];
      }
    }
    return null;
  }

  @Nullable
  private static String currentMajorMinor() {
    String version = PinotVersion.VERSION;
    if (version == null || PinotVersion.UNKNOWN.equals(version)) {
      return null;
    }
    return majorMinor(version);
  }

  /// Returns the `major.minor` prefix of a version string, stripping any pre-release qualifier (e.g. `-SNAPSHOT`)
  /// and ignoring the patch component. Returns `null` if the input does not parse.
  @Nullable
  static String majorMinor(@Nullable String version) {
    if (version == null) {
      return null;
    }
    String trimmed = version.trim();
    int qualifierIdx = trimmed.indexOf('-');
    if (qualifierIdx >= 0) {
      trimmed = trimmed.substring(0, qualifierIdx);
    }
    String[] parts = trimmed.split("\\.");
    if (parts.length < 2) {
      return null;
    }
    if (!isNumeric(parts[0]) || !isNumeric(parts[1])) {
      return null;
    }
    return parts[0] + "." + parts[1];
  }

  private static boolean isNumeric(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /// Decides the severity for a violation based on the annotation's `since` value. The current Pinot release's
  /// deprecations are warnings (one-release grace period); older deprecations escalate to errors.
  ///
  /// Feature gate: while `true`, every parseable `@DeprecatedConfig.since` classifies as a WARNING regardless of
  /// the running Pinot version. Flip to `false` (and supply real comparison logic in [#classifySeverity]) to
  /// promote older deprecations to ERROR after the codebase has migrated off them. Grep-able sentinel so the
  /// follow-up promotion PR is a single-keyword change rather than a behavioural drift.
  ///
  /// PROMOTION PRE-CONDITIONS — the follow-up PR that flips this MUST first land:
  /// 1. A version-checked CAS on every update path that currently uses best-effort read-then-write for the
  ///    deprecation diff. The relevant call sites are tagged with `// TODO(SOFT_LAUNCH_WARNING_ONLY)` so a
  ///    grep at flip-time surfaces every one.
  /// 2. A concurrency regression test that exercises the read→write window with a stubbed ZK observer.
  /// 3. A test seam (e.g. injected currentMajorMinor or a non-final flag) that exercises this method's
  ///    older-than-current → ERROR branch end-to-end.
  static final boolean SOFT_LAUNCH_WARNING_ONLY = true;

  /// Classifies the soft-launch severity for a deprecated property. Under the [#SOFT_LAUNCH_WARNING_ONLY] policy
  /// the only outcome that is ever ERROR is "the annotation's `since` string is unparseable" — a code-side bug,
  /// not a user-supplied data problem. Every other case (including the version-unknown environment fallback) is
  /// reported as WARNING.
  static Severity classifySeverity(String since) {
    return classifySeverity(since, currentMajorMinor());
  }

  /// Test seam for [#classifySeverity(String)] that takes the current major.minor version explicitly so the
  /// version-unknown fallback branch can be unit-tested without manipulating the classloader-scoped
  /// `pinot-version.properties` resource.
  ///
  /// Note: the `currentMajorMinor` parameter is currently unused at production runtime because
  /// [#SOFT_LAUNCH_WARNING_ONLY] forces every parseable `since` to WARNING. It is preserved for the follow-up
  /// promotion PR which will compare against the running version to decide ERROR vs WARNING.
  static Severity classifySeverity(String since, @Nullable String currentMajorMinor) {
    String sinceMajorMinor = majorMinor(since);
    if (sinceMajorMinor == null) {
      return Severity.ERROR;
    }
    return Severity.WARNING;
  }

  /// Returns true when the JSON value matches the Java zero-value for its type (`false` for booleans, `0` for
  /// numerics, empty for strings/arrays/objects, and explicit JSON `null`). This is an approximation of
  /// Jackson's `@JsonInclude(NON_DEFAULT)` semantics: the real Jackson behaviour consults the bean's actual
  /// initialised default by instantiating it, but every deprecated getter currently annotated with
  /// `@DeprecatedConfig` uses a type-zero default. A regression test in
  /// `DeprecatedTableConfigValidationUtilsTest#testEveryAnnotatedGetterHasJavaZeroDefault` walks every
  /// discovered rule's leaf bean+getter and asserts the Java default value is treated as default here, so a
  /// future contributor who annotates a non-zero-default getter fails at test time rather than silently
  /// suppressing diffs at runtime.
  ///
  /// Numeric types are compared via integer paths where possible to avoid the `double`-coercion edge cases
  /// (lossy conversion for `BigDecimal`/`BigInteger`, IEEE-754 `-0.0 == 0.0d`, etc.). Today no annotated getter
  /// returns a floating-point or arbitrary-precision type, but the dispatch is robust enough to extend to those
  /// cases in the future.
  static boolean isJacksonDefault(@Nullable JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return true;
    }
    if (node.isBoolean()) {
      return !node.booleanValue();
    }
    if (node.isShort() || node.isInt() || node.isLong()) {
      return node.longValue() == 0L;
    }
    if (node.isBigInteger()) {
      return BigInteger.ZERO.equals(node.bigIntegerValue());
    }
    if (node.isBigDecimal()) {
      return BigDecimal.ZERO.compareTo(node.decimalValue()) == 0;
    }
    if (node.isFloatingPointNumber()) {
      // Use Double.compare so -0.0 is treated as non-default (it differs from the Java zero-value of 0.0d).
      return Double.compare(node.doubleValue(), 0.0d) == 0;
    }
    if (node.isTextual()) {
      // Every currently-annotated String-returning getter has a Java field default of null (asserted by
      // DeprecatedTableConfigValidationUtilsTest#testEveryAnnotatedGetterHasJavaZeroDefault). The explicit-null
      // case is already handled above, so any textual node here is a real user-supplied value. NOTE: this is NOT
      // a faithful reimplementation of Jackson @JsonInclude(NON_DEFAULT) — a String field that defaults to "" in
      // its bean would be elided by Jackson and would arrive as a missing node, never reaching this branch. If a
      // future @DeprecatedConfig is added on a String getter whose owning field defaults to "", revisit this
      // branch and the no-arg-default-instance test that backs it.
      return false;
    }
    if (node.isArray() || node.isObject()) {
      return node.isEmpty();
    }
    // BinaryNode / POJONode never arise from text JSON parsed by Jackson; treat as non-default conservatively.
    return false;
  }

  static final class DeprecatedConfigRule {
    private final List<String> _pathSegments;
    private final String _replacement;
    private final String _since;
    private final Severity _severity;
    private final Class<?> _leafOwner;
    private final Method _leafGetter;

    DeprecatedConfigRule(List<String> pathSegments, String replacement, String since, Severity severity,
        Class<?> leafOwner, Method leafGetter) {
      _pathSegments = pathSegments;
      _replacement = replacement;
      _since = since;
      _severity = severity;
      _leafOwner = leafOwner;
      _leafGetter = leafGetter;
    }

    List<String> pathSegments() {
      return _pathSegments;
    }

    Severity severity() {
      return _severity;
    }

    /// Returns the bean class that declares the leaf annotated getter. Exposed for tests so they can verify the
    /// default-value invariant assumed by [#isJacksonDefault].
    Class<?> leafOwner() {
      return _leafOwner;
    }

    /// Returns the leaf annotated getter. Exposed for tests so they can read the Java default value from a
    /// no-arg-constructed bean and assert it is treated as default by [#isJacksonDefault].
    Method leafGetter() {
      return _leafGetter;
    }

    void collect(JsonNode newRoot, @Nullable JsonNode oldRoot, String pathPrefix, List<String> errors,
        List<String> warnings) {
      List<MatchedPath> matches = new ArrayList<>();
      collectMatches(newRoot, oldRoot, null, 0, pathPrefix, matches);
      for (MatchedPath match : matches) {
        if (oldRoot != null) {
          // Update path. Silently drop matches that are unchanged from the stored config, OR that are absent in
          // the stored config but whose new value is the type's Java default. The default-skip handles the case
          // where many deprecated getters carry @JsonInclude(NON_DEFAULT): a previous create with
          // `enableSnapshot: false` is stripped at ZK write time, so on PUT the diff would otherwise see `false`
          // as "newly introduced" and reject an unchanged config. Crucially, the default-skip applies ONLY when
          // the stored config did not contain the path — a deliberate flip of an existing non-default value back
          // to the default (e.g. `true` → `false`) is still reported, matching validateOnUpdate's contract that
          // value-changed deprecated paths must be flagged.
          if (match._oldValue != null) {
            if (Objects.equals(match._newValue, match._oldValue)) {
              continue;
            }
          } else if (isModernFieldConfigIndexTypeEquivalent(match)
              || DeprecatedTableConfigValidationUtils.isJacksonDefault(match._newValue)) {
            continue;
          }
        }
        String message = "'" + match._path + "' is deprecated since " + _since + ". " + _replacement;
        if (_severity == Severity.ERROR) {
          errors.add(message);
        } else {
          warnings.add(message);
        }
      }
    }

    private boolean isModernFieldConfigIndexTypeEquivalent(MatchedPath match) {
      if (!_pathSegments.equals(FIELD_CONFIG_INDEX_TYPE_PATH) || match._oldParent == null
          || !match._oldParent.isObject()) {
        return false;
      }
      JsonNode oldIndexTypes = match._oldParent.get(FIELD_CONFIG_INDEX_TYPES_KEY);
      return oldIndexTypes != null && oldIndexTypes.isArray() && oldIndexTypes.size() == 1
          && Objects.equals(oldIndexTypes.get(0), match._newValue);
    }

    private void collectMatches(@Nullable JsonNode newNode, @Nullable JsonNode oldNode,
        @Nullable JsonNode oldParentNode, int idx, String currentPath, List<MatchedPath> matches) {
      if (newNode == null || newNode.isMissingNode()) {
        return;
      }
      if (idx == _pathSegments.size()) {
        matches.add(new MatchedPath(currentPath, newNode, oldNode, oldParentNode));
        return;
      }
      String segment = _pathSegments.get(idx);
      if (WILDCARD.equals(segment)) {
        if (newNode.isArray()) {
          for (int i = 0; i < newNode.size(); i++) {
            JsonNode newElem = newNode.get(i);
            // Find the matching old element by a stable identity key so that reordering an array between two PUTs
            // does not surface unchanged legacy values as "newly introduced" — see C5.2 / validateOnUpdate
            // contract. The lookup prefers the `name` field (used by FieldConfig, TierConfig, etc.); falls back to
            // positional alignment only when neither side has a usable identity key.
            JsonNode oldElem = findMatchingOldElement(newElem, oldNode, i);
            collectMatches(newElem, oldElem, oldNode, idx + 1, currentPath + "[" + i + "]", matches);
          }
        } else if (newNode.isObject()) {
          newNode.fieldNames().forEachRemaining(field -> {
            JsonNode oldChild = (oldNode != null && oldNode.isObject()) ? oldNode.get(field) : null;
            collectMatches(newNode.get(field), oldChild, oldNode, idx + 1, append(currentPath, field), matches);
          });
        }
        return;
      }
      if (newNode.has(segment)) {
        JsonNode oldChild = (oldNode != null && oldNode.isObject()) ? oldNode.get(segment) : null;
        collectMatches(newNode.get(segment), oldChild, oldNode, idx + 1, append(currentPath, segment), matches);
      }
    }

    @Nullable
    private static JsonNode findMatchingOldElement(JsonNode newElem, @Nullable JsonNode oldNode, int positionalIdx) {
      if (oldNode == null || !oldNode.isArray()) {
        return null;
      }
      // Identity-key match policy: `name` is the stable key for FieldConfig and TierConfig (the Pinot
      // table-config beans that ship as wildcard-element types under TableConfig today). To avoid a
      // positional-fallback footgun where a malformed legacy element without a `name` could spuriously align
      // with a different-shaped old element at the same index, require either:
      // - BOTH sides carry a textual `name` (match by name; null if no equal-name candidate exists), OR
      // - NEITHER side carries a textual `name` (positional fallback for genuinely non-keyed arrays).
      // Mixed shapes (one side has `name`, the other doesn't) return null so the new element is treated as
      // newly-introduced rather than incorrectly aligned with a positional sibling.
      boolean newHasName = hasTextualName(newElem);
      boolean anyOldHasName = false;
      for (int i = 0; i < oldNode.size(); i++) {
        if (hasTextualName(oldNode.get(i))) {
          anyOldHasName = true;
          break;
        }
      }
      if (newHasName != anyOldHasName) {
        // Asymmetric: don't pretend the position-aligned sibling is the same element.
        return null;
      }
      if (newHasName) {
        String key = newElem.get("name").asText();
        for (int i = 0; i < oldNode.size(); i++) {
          JsonNode candidate = oldNode.get(i);
          if (hasTextualName(candidate) && key.equals(candidate.get("name").asText())) {
            return candidate;
          }
        }
        // newElem has a name but no old element matches by name → treat as newly introduced (return null).
        return null;
      }
      return positionalIdx < oldNode.size() ? oldNode.get(positionalIdx) : null;
    }

    private static boolean hasTextualName(@Nullable JsonNode node) {
      return node != null && node.isObject() && node.hasNonNull("name") && node.get("name").isTextual();
    }

    private static String append(String currentPath, String segment) {
      return currentPath.isEmpty() ? segment : currentPath + "." + segment;
    }
  }

  private static final class MatchedPath {
    final String _path;
    final JsonNode _newValue;
    @Nullable
    final JsonNode _oldValue;
    @Nullable
    final JsonNode _oldParent;

    MatchedPath(String path, JsonNode newValue, @Nullable JsonNode oldValue, @Nullable JsonNode oldParent) {
      _path = path;
      _newValue = newValue;
      _oldValue = oldValue;
      _oldParent = oldParent;
    }
  }
}
