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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/// Produces the display-safe form of a table config and merges that form back into the stored config on update.
///
/// Redaction is recursive so plugin-owned stream, batch, filesystem, decoder, provider, task, and custom property maps
/// receive the same treatment as first-class table config fields. Literal values under credential-bearing keys are
/// masked, as are JAAS/connection-string assignments and credentials embedded in URI user-info or query parameters.
/// Whole-value environment and system-property placeholders are preserved and are never resolved by this class.
///
/// The redaction marker is reserved in credential-bearing positions. During an update, an unchanged marker restores
/// the corresponding value from the unresolved stored config. Marker-bearing array elements are matched to their
/// exact redacted stored form instead of by array position, which supports reordering without moving one source's
/// credentials to another. Ambiguous or edited marker-bearing elements are rejected.
///
/// This class is stateless and thread-safe.
public final class TableConfigRedactionUtils {
  public static final String REDACTION_MARKER = "*****";

  private static final Pattern URI_REFERENCE_PATTERN = Pattern.compile("(?i)(?:[a-z][a-z0-9+.-]*:)?//");
  private static final Pattern CREDENTIAL_ASSIGNMENT_PATTERN = Pattern.compile(
      "(?i)(\\b([a-z][a-z0-9._-]*)\\s*=\\s*)"
          + "(\\\"(?:\\\\.|[^\\\"\\\\]++)*+\\\"|'(?:\\\\.|[^'\\\\]++)*+'|"
          + "\\\"(?:\\\\.|[^\\\"\\\\]++)*+(?=;|\\r|\\n|$)|"
          + "'(?:\\\\.|[^'\\\\]++)*+(?=;|\\r|\\n|$)|"
          + "(?:(?![&,][a-z][a-z0-9._-]*\\s*=)(?!\\s+[a-z][a-z0-9._-]*\\s*=(?!=))"
          + "[^;\\r\\n'\"])+)");
  private static final Pattern JAAS_MODULE_PATTERN = Pattern.compile(
      "(?i)([a-z_$][a-z0-9_.$-]*)\\s+(?:required|requisite|sufficient|optional)\\b");

  private TableConfigRedactionUtils() {
  }

  /// Returns a deep-copied table config with displayable credentials replaced by [#REDACTION_MARKER].
  public static TableConfig redact(TableConfig tableConfig) {
    Objects.requireNonNull(tableConfig, "tableConfig must not be null");
    JsonNode redacted = redactNode(tableConfig.toJsonNode(), null, false);
    return toTableConfig(redacted, "Unable to create redacted table config");
  }

  /// Redacts credential-bearing assignments and URI components from an arbitrary display string.
  ///
  /// This is the same leaf-value policy used by [#redact(TableConfig)]. Whole placeholders and ordinary text are
  /// preserved. JSON-encoded object and array values are traversed recursively so plugin option blobs receive the
  /// same key policy as native property maps.
  public static String redactStructuredText(String value) {
    Objects.requireNonNull(value, "value must not be null");
    if (isPlaceholder(value)) {
      return value;
    }
    return redactStructuredCredentials(value);
  }

  /// Returns whether a property name is classified as credential-bearing by the shared display policy.
  public static boolean isSensitivePropertyName(String propertyName) {
    return propertyName != null && isSensitiveKey(propertyName);
  }

  /// Returns whether the whole value is an unresolved environment or system-property placeholder.
  public static boolean isUnresolvedPlaceholder(String value) {
    return value != null && isPlaceholder(value);
  }

  /// Returns a deep-copied submitted config with unchanged redaction markers restored from the unresolved stored
  /// config. New literal values are retained as intentional credential replacements.
  ///
  /// @throws IllegalArgumentException if a marker has no unambiguous stored credential to restore
  public static TableConfig restoreRedactedValues(TableConfig submitted, @Nullable TableConfig stored) {
    Objects.requireNonNull(submitted, "submitted table config must not be null");
    JsonNode submittedNode = submitted.toJsonNode();
    JsonNode storedNode = stored != null ? stored.toJsonNode() : null;
    JsonNode restored = restoreNode(submittedNode, storedNode, null, false);
    return toTableConfig(restored, "Unable to restore redacted table config");
  }

  /// Redacts credentials from a diagnostic that was produced while handling the supplied table config. This preserves
  /// useful validation context without allowing literal config credentials to reach an error response or log record.
  public static String redactDiagnostic(@Nullable String diagnostic, TableConfig tableConfig) {
    Objects.requireNonNull(tableConfig, "tableConfig must not be null");
    if (diagnostic == null || diagnostic.isEmpty()) {
      return "Invalid table config";
    }
    // A diagnostic can contain the resolved value of a placeholder even though the unresolved config never contains
    // that literal. There is no sound string-level way to discover and remove such a value, so fail closed.
    if (containsPlaceholder(tableConfig.toJsonNode())) {
      return "Invalid table config";
    }
    String redacted = redactStructuredCredentials(diagnostic);
    List<String> literalCredentials = new ArrayList<>();
    collectLiteralCredentials(tableConfig.toJsonNode(), null, false, literalCredentials);
    literalCredentials.sort(Comparator.comparingInt(String::length).reversed());
    for (String credential : literalCredentials) {
      if (!credential.isEmpty() && !isPlaceholder(credential)) {
        redacted = redacted.replace(credential, REDACTION_MARKER);
        redacted = redacted.replace(jsonEscaped(credential), REDACTION_MARKER);
      }
    }
    return redacted;
  }

  private static String jsonEscaped(String value) {
    try {
      String json = JsonUtils.objectToString(value);
      return json.substring(1, json.length() - 1);
    } catch (IOException e) {
      return value;
    }
  }

  /// Returns whether replacing the JSON Pointer target with the supplied literal would reproduce its current masked
  /// representation under this policy. This permits whole-leaf JAAS and URI replacements without allowing an override
  /// to change non-credential content such as a URI host or JAAS principal.
  public static boolean isValidCredentialOverride(JsonNode tableConfigNode, String jsonPointer, String replacement) {
    Objects.requireNonNull(tableConfigNode, "tableConfigNode must not be null");
    Objects.requireNonNull(jsonPointer, "jsonPointer must not be null");
    Objects.requireNonNull(replacement, "replacement must not be null");
    String[] segments = jsonPointer.split("/", -1);
    if (segments.length <= 1 || !segments[0].isEmpty()) {
      return false;
    }
    JsonNode node = tableConfigNode;
    boolean parentSensitive = false;
    String fieldName = null;
    for (int i = 1; i < segments.length; i++) {
      String segment = decodeJsonPointerSegment(segments[i]);
      if (node.isObject()) {
        fieldName = segment;
        parentSensitive |= isSensitiveKey(fieldName);
        node = node.get(fieldName);
      } else if (node.isArray()) {
        int index;
        try {
          index = Integer.parseInt(segment);
        } catch (NumberFormatException e) {
          return false;
        }
        node = index >= 0 && index < node.size() ? node.get(index) : null;
      } else {
        return false;
      }
      if (node == null) {
        return false;
      }
    }
    if (!node.isTextual() || isPlaceholder(node.textValue())) {
      return false;
    }
    boolean hasPolicyMarker = parentSensitive && REDACTION_MARKER.equals(node.textValue())
        || containsCredentialMarker(node.textValue());
    return hasPolicyMarker && redactText(fieldName, replacement, parentSensitive).equals(node.textValue());
  }

  private static String decodeJsonPointerSegment(String segment) {
    return segment.replace("~1", "/").replace("~0", "~");
  }

  private static boolean containsPlaceholder(JsonNode node) {
    if (node.isObject()) {
      for (JsonNode value : node) {
        if (containsPlaceholder(value)) {
          return true;
        }
      }
      return false;
    }
    if (node.isArray()) {
      for (JsonNode value : node) {
        if (containsPlaceholder(value)) {
          return true;
        }
      }
      return false;
    }
    return node.isTextual() && node.textValue().contains("${");
  }

  private static void collectLiteralCredentials(JsonNode node, @Nullable String fieldName, boolean parentSensitive,
      List<String> credentials) {
    boolean sensitive = parentSensitive || isSensitiveKey(fieldName);
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> field : node.properties()) {
        collectLiteralCredentials(field.getValue(), field.getKey(), sensitive, credentials);
      }
      return;
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        collectLiteralCredentials(element, fieldName, sensitive, credentials);
      }
      return;
    }
    if (!node.isValueNode()) {
      return;
    }
    if (sensitive) {
      if (!node.isTextual() || !isPlaceholder(node.textValue())) {
        credentials.add(node.asText());
      }
    } else if (node.isTextual() && !isPlaceholder(node.textValue())) {
      collectStructuredLiteralCredentials(node.textValue(), credentials);
    }
  }

  private static void collectStructuredLiteralCredentials(String value, List<String> credentials) {
    JsonNode structuredJson = tryParseStructuredJson(value);
    if (structuredJson != null) {
      collectLiteralCredentials(structuredJson, null, false, credentials);
      return;
    }
    List<UriSpan> uriSpans = findUriSpans(value);
    int offset = 0;
    for (UriSpan span : uriSpans) {
      collectAssignmentLiteralCredentials(value.substring(offset, span._start), credentials);
      collectUriLiteralCredentials(value.substring(span._start, span._end), credentials);
      offset = span._end;
    }
    collectAssignmentLiteralCredentials(value.substring(offset), credentials);
  }

  private static void collectAssignmentLiteralCredentials(String value, List<String> credentials) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(value);
    while (matcher.find()) {
      String credential = unquote(matcher.group(3));
      if (isSensitiveKey(matcher.group(2)) && !isPlaceholder(credential)) {
        credentials.add(credential);
      }
    }
  }

  private static void collectUriLiteralCredentials(String uri, List<String> credentials) {
    UriParts parts = new UriParts(uri);
    if (parts._userInfo != null) {
      for (String credential : splitOutsidePlaceholders(parts._userInfo, ':')) {
        if (!credential.isEmpty() && !isPlaceholder(credential)) {
          credentials.add(credential);
        }
      }
    }
    collectUriParameterLiteralCredentials(parts._query, credentials);
    collectUriParameterLiteralCredentials(parameterLikeFragment(parts._fragment), credentials);
  }

  private static void collectUriParameterLiteralCredentials(@Nullable String parameters,
      List<String> credentials) {
    if (parameters == null) {
      return;
    }
    for (String parameter : splitQuery(parameters)) {
      int equalsIndex = parameter.indexOf('=');
      if (equalsIndex < 0) {
        continue;
      }
      String key = stripQueryPrefix(parameter.substring(0, equalsIndex));
      String value = parameter.substring(equalsIndex + 1);
      if (isSensitiveQueryKey(key) && !isPlaceholder(value)) {
        credentials.add(value);
        String decoded = decodeQueryValue(value);
        if (decoded != null && !decoded.equals(value)) {
          credentials.add(decoded);
        }
      } else if (!isPlaceholder(value)) {
        collectStructuredLiteralCredentials(value, credentials);
        String decoded = decodeQueryValue(value);
        if (decoded != null && !decoded.equals(value)) {
          collectStructuredLiteralCredentials(decoded, credentials);
        }
      }
    }
  }

  private static JsonNode redactNode(JsonNode node, @Nullable String fieldName, boolean parentSensitive) {
    boolean sensitive = parentSensitive || isSensitiveKey(fieldName);
    if (node.isObject()) {
      ObjectNode copy = (ObjectNode) node.deepCopy();
      for (Map.Entry<String, JsonNode> field : copy.properties()) {
        copy.set(field.getKey(), redactNode(field.getValue(), field.getKey(), sensitive));
      }
      return copy;
    }
    if (node.isArray()) {
      ArrayNode copy = (ArrayNode) node.deepCopy();
      for (int i = 0; i < copy.size(); i++) {
        copy.set(i, redactNode(copy.get(i), fieldName, sensitive));
      }
      return copy;
    }
    if (node.isTextual()) {
      return JsonUtils.objectToJsonNode(redactText(fieldName, node.textValue(), sensitive));
    }
    if (sensitive && node.isValueNode() && !node.isNull()) {
      return JsonUtils.objectToJsonNode(REDACTION_MARKER);
    }
    return node.deepCopy();
  }

  private static JsonNode restoreNode(JsonNode submitted, @Nullable JsonNode stored, @Nullable String fieldName,
      boolean parentSensitive) {
    boolean sensitive = parentSensitive || isSensitiveKey(fieldName);
    if (submitted.isObject()) {
      validateObjectSecurityIdentity((ObjectNode) submitted, stored, fieldName, sensitive);
      ObjectNode copy = (ObjectNode) submitted.deepCopy();
      for (Map.Entry<String, JsonNode> field : copy.properties()) {
        JsonNode storedChild = stored != null && stored.isObject() ? stored.get(field.getKey()) : null;
        copy.set(field.getKey(), restoreNode(field.getValue(), storedChild, field.getKey(), sensitive));
      }
      return copy;
    }
    if (submitted.isArray()) {
      return restoreArray((ArrayNode) submitted, stored, fieldName, sensitive);
    }
    if (sensitive && submitted.isTextual() && REDACTION_MARKER.equals(submitted.textValue())) {
      if (stored == null || stored.isContainerNode()) {
        throw unresolvedMarker();
      }
      return stored.deepCopy();
    }
    if (!submitted.isTextual()) {
      return submitted.deepCopy();
    }

    String submittedValue = submitted.textValue();
    String storedValue = stored != null && stored.isTextual() ? stored.textValue() : null;
    if (isPlaceholder(submittedValue)) {
      return submitted.deepCopy();
    }
    if (sensitive) {
      if (submittedValue.contains(REDACTION_MARKER)) {
        throw unresolvedMarker();
      }
      return submitted.deepCopy();
    }

    if (storedValue != null) {
      String redactedStoredValue = redactText(fieldName, storedValue, false);
      if (!storedValue.equals(redactedStoredValue) && submittedValue.equals(redactedStoredValue)) {
        return JsonUtils.objectToJsonNode(storedValue);
      }
    }
    if (containsCredentialMarker(submittedValue)) {
      if (storedValue == null) {
        throw unresolvedMarker();
      }
      return JsonUtils.objectToJsonNode(restoreStructuredCredentials(submittedValue, storedValue));
    }
    return submitted.deepCopy();
  }

  private static JsonNode restoreArray(ArrayNode submitted, @Nullable JsonNode stored, @Nullable String fieldName,
      boolean sensitive) {
    ArrayNode copy = (ArrayNode) submitted.deepCopy();
    if (stored != null && stored.isArray() && submitted.equals(redactNode(stored, fieldName, sensitive))) {
      for (int i = 0; i < copy.size(); i++) {
        copy.set(i, restoreNode(copy.get(i), stored.get(i), fieldName, sensitive));
      }
      return copy;
    }
    if (canRestoreArrayByPosition(submitted, stored, fieldName, sensitive)) {
      for (int i = 0; i < copy.size(); i++) {
        copy.set(i, restoreNode(copy.get(i), stored.get(i), fieldName, sensitive));
      }
      return copy;
    }
    boolean[] usedStoredElements = stored != null && stored.isArray() ? new boolean[stored.size()] : new boolean[0];
    for (int i = 0; i < copy.size(); i++) {
      JsonNode submittedElement = copy.get(i);
      int match = -1;
      if (containsRestorableMarker(submittedElement, fieldName, sensitive)) {
        match = findExactStoredArrayElement(submittedElement, stored, fieldName, sensitive, usedStoredElements);
        if (match >= 0) {
          usedStoredElements[match] = true;
        }
      }
      if (match < 0 && containsRestorableMarker(submittedElement, fieldName, sensitive)) {
        throw unresolvedMarker();
      }
      JsonNode storedElement = match >= 0 ? stored.get(match) : null;
      copy.set(i, restoreNode(submittedElement, storedElement, fieldName, sensitive));
    }
    return copy;
  }

  private static boolean canRestoreArrayByPosition(ArrayNode submitted, @Nullable JsonNode stored,
      @Nullable String fieldName, boolean sensitive) {
    if (stored == null || !stored.isArray() || submitted.size() != stored.size()) {
      return false;
    }
    if (submitted.size() <= 1) {
      if (submitted.isEmpty() || !containsRestorableMarker(submitted.get(0), fieldName, sensitive)) {
        return true;
      }
      Map<String, String> submittedIdentity = new HashMap<>();
      collectStableArrayIdentity(submitted.get(0), "", fieldName, sensitive, submittedIdentity);
      if (submittedIdentity.isEmpty()) {
        return false;
      }
      Map<String, String> storedIdentity = new HashMap<>();
      collectStableArrayIdentity(redactNode(stored.get(0), fieldName, sensitive), "", fieldName, sensitive,
          storedIdentity);
      return submittedIdentity.equals(storedIdentity);
    }
    Set<Map<String, String>> submittedIdentities = new HashSet<>();
    for (int i = 0; i < submitted.size(); i++) {
      if (!containsRestorableMarker(submitted.get(i), fieldName, sensitive)) {
        continue;
      }
      Map<String, String> submittedIdentity = new HashMap<>();
      collectStableArrayIdentity(submitted.get(i), "", fieldName, sensitive, submittedIdentity);
      if (submittedIdentity.isEmpty()) {
        return false;
      }
      Map<String, String> storedIdentity = new HashMap<>();
      collectStableArrayIdentity(redactNode(stored.get(i), fieldName, sensitive), "", fieldName, sensitive,
          storedIdentity);
      if (!submittedIdentity.equals(storedIdentity) || !submittedIdentities.add(submittedIdentity)) {
        return false;
      }
    }
    Set<Map<String, String>> storedIdentities = new HashSet<>();
    for (int i = 0; i < stored.size(); i++) {
      JsonNode redactedStored = redactNode(stored.get(i), fieldName, sensitive);
      if (!containsRestorableMarker(redactedStored, fieldName, sensitive)) {
        continue;
      }
      Map<String, String> identity = new HashMap<>();
      collectStableArrayIdentity(redactedStored, "", fieldName, sensitive, identity);
      if (identity.isEmpty()) {
        return false;
      }
      if (!storedIdentities.add(identity)) {
        return false;
      }
    }
    return true;
  }

  private static int findExactStoredArrayElement(JsonNode submittedElement, @Nullable JsonNode stored,
      @Nullable String fieldName, boolean sensitive, boolean[] usedStoredElements) {
    if (stored == null || !stored.isArray()) {
      return -1;
    }
    int candidate = -1;
    int candidateCount = 0;
    for (int i = 0; i < stored.size(); i++) {
      if (usedStoredElements[i]) {
        continue;
      }
      if (!submittedElement.equals(redactNode(stored.get(i), fieldName, sensitive))) {
        continue;
      }
      candidate = i;
      candidateCount++;
    }
    return candidateCount == 1 ? candidate : -1;
  }

  private static void collectStableArrayIdentity(JsonNode node, String path, @Nullable String fieldName,
      boolean parentSensitive, Map<String, String> identity) {
    boolean sensitive = parentSensitive || isSensitiveKey(fieldName);
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> field : node.properties()) {
        String childPath = path.isEmpty() ? field.getKey() : path + "." + field.getKey();
        collectStableArrayIdentity(field.getValue(), childPath, field.getKey(), sensitive, identity);
      }
      return;
    }
    if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        collectStableArrayIdentity(node.get(i), path + "[" + i + "]", fieldName, sensitive, identity);
      }
      return;
    }
    if (!sensitive && isStableArrayIdentityKey(fieldName) && !containsRestorableMarker(node, fieldName, false)) {
      identity.put(path, stableIdentityValue(node));
    }
  }

  private static boolean isStableArrayIdentityKey(@Nullable String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String normalized = normalizeKey(fieldName);
    return normalized.equals("name") || normalized.equals("id") || normalized.equals("type")
        || normalized.endsWith("tablename") || normalized.endsWith("tasktype")
        || normalized.endsWith("streamtype") || normalized.endsWith("topic")
        || normalized.endsWith("topicname") || normalized.endsWith("endpoint")
        || normalized.endsWith("host") || normalized.endsWith("server") || normalized.endsWith("url")
        || normalized.endsWith("uri") || normalized.endsWith("path") || normalized.endsWith("bootstrapservers")
        || isExecutableClassSelectorKey(normalized);
  }

  private static void validateObjectSecurityIdentity(ObjectNode submitted, @Nullable JsonNode stored,
      @Nullable String fieldName, boolean parentSensitive) {
    if (stored == null || !stored.isObject()
        || !containsRestorableMarker(submitted, fieldName, parentSensitive)) {
      return;
    }
    ObjectNode redactedStored = (ObjectNode) redactNode(stored, fieldName, parentSensitive);
    rejectNormalizedKeyAliases(submitted);
    Map<String, String> submittedIdentity = new HashMap<>();
    collectObjectSecurityIdentity(submitted, "", fieldName, parentSensitive, submittedIdentity);
    Map<String, String> storedIdentity = new HashMap<>();
    collectObjectSecurityIdentity(redactedStored, "", fieldName, parentSensitive, storedIdentity);
    if (!submittedIdentity.equals(storedIdentity)) {
      throw unresolvedMarker();
    }
    validateCredentialPairs(submitted, redactedStored);
    validateCredentialEndpointBindings(submitted, redactedStored);
  }

  private static void rejectNormalizedKeyAliases(JsonNode node) {
    if (node.isObject()) {
      Map<String, String> normalizedToRaw = new HashMap<>();
      for (Map.Entry<String, JsonNode> field : node.properties()) {
        String normalized = normalizeKey(field.getKey());
        String previous = normalizedToRaw.putIfAbsent(normalized, field.getKey());
        if (previous != null && !previous.equals(field.getKey())) {
          throw unresolvedMarker();
        }
        rejectNormalizedKeyAliases(field.getValue());
      }
    } else if (node.isArray()) {
      for (JsonNode element : node) {
        rejectNormalizedKeyAliases(element);
      }
    }
  }

  private static void collectObjectSecurityIdentity(JsonNode node, String path, @Nullable String fieldName,
      boolean parentSensitive, Map<String, String> identity) {
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> field : node.properties()) {
        String childPath = path.isEmpty() ? field.getKey() : path + "." + field.getKey();
        JsonNode value = field.getValue();
        if (isObjectOwnerIdentityField(field.getKey()) && value.isValueNode()
            && !containsRestorableMarker(value, field.getKey(), parentSensitive)) {
          identity.put(childPath, stableIdentityValue(value));
        }
        collectObjectSecurityIdentity(value, childPath, field.getKey(), parentSensitive, identity);
      }
      return;
    }
    if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        collectObjectSecurityIdentity(node.get(i), path + "[" + i + "]", fieldName, parentSensitive, identity);
      }
    }
  }

  private static boolean isObjectOwnerIdentityField(@Nullable String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String normalized = normalizeKey(fieldName);
    return normalized.equals("user") || normalized.equals("username") || normalized.equals("principal")
        || normalized.equals("accountname")
        || isExecutableClassSelectorKey(normalized)
        || normalized.endsWith("clientemail");
  }

  private static boolean isExecutableClassSelectorKey(String normalizedKey) {
    return normalizedKey.equals("class") || normalizedKey.endsWith("interceptorclasses")
        || normalizedKey.endsWith("metricreporters") || normalizedKey.endsWith("partitionassignmentstrategy")
        || normalizedKey.endsWith("deserializer") || normalizedKey.endsWith("serializer")
        || normalizedKey.endsWith("partitionerclass") || normalizedKey.endsWith("oauthprovidertype")
        || normalizedKey.contains("fsazureaccountoauthprovidertype")
        || normalizedKey.endsWith("securityproviders")
        || normalizedKey.endsWith("classname")
        || normalizedKey.endsWith("factoryclass") || normalizedKey.endsWith("providerclass")
        || normalizedKey.endsWith("decoderclass") || normalizedKey.endsWith("callbackhandlerclass")
        || normalizedKey.endsWith("loginmoduleclass") || normalizedKey.endsWith("loginclass")
        || normalizedKey.endsWith("credentialsprovider")
        || normalizedKey.endsWith("credentialprovider");
  }

  private static void validateCredentialPairs(ObjectNode submitted, ObjectNode redactedStored) {
    Map<String, JsonNode> submittedFields = normalizedFields(submitted);
    Map<String, JsonNode> storedFields = normalizedFields(redactedStored);
    validateCredentialPair(submittedFields, storedFields,
        List.of("accesskey", "accesskeyid", "awsaccesskeyid"), List.of("secretkey", "secretaccesskey"));
    validateCredentialPair(submittedFields, storedFields,
        List.of("clientid", "applicationid"), List.of("clientsecret"));
    validateCredentialPair(submittedFields, storedFields,
        List.of("sharedaccesskeyname", "sharedaccesspolicyname"), List.of("sharedaccesskey"));
    validateCredentialPair(submittedFields, storedFields,
        List.of("username", "user", "principal", "accountname"),
        List.of("passwords", "password", "passwd", "pwd", "passphrases", "passphrase"));
    for (Map.Entry<String, JsonNode> field : submitted.properties()) {
      JsonNode storedChild = redactedStored.get(field.getKey());
      if (field.getValue().isObject() && storedChild != null && storedChild.isObject()) {
        validateCredentialPairs((ObjectNode) field.getValue(), (ObjectNode) storedChild);
      }
    }
  }

  private static void validateCredentialEndpointBindings(ObjectNode submitted, ObjectNode redactedStored) {
    Map<String, String> submittedEndpoints = collectEndpointBindings(submitted);
    Map<String, String> storedEndpoints = collectEndpointBindings(redactedStored);
    for (Map.Entry<String, JsonNode> field : submitted.properties()) {
      String normalizedKey = normalizeKey(field.getKey());
      String credentialSuffix = sensitiveBindingSuffix(normalizedKey);
      if (credentialSuffix == null || !containsRestorableMarker(field.getValue(), field.getKey(), false)) {
        continue;
      }
      String prefix = normalizedKey.substring(0, normalizedKey.length() - credentialSuffix.length());
      if (!endpointBindingsForPrefix(submittedEndpoints, prefix)
          .equals(endpointBindingsForPrefix(storedEndpoints, prefix))) {
        throw unresolvedMarker();
      }
    }
  }

  private static Map<String, String> collectEndpointBindings(ObjectNode node) {
    Map<String, String> endpoints = new HashMap<>();
    for (Map.Entry<String, JsonNode> field : node.properties()) {
      String normalizedKey = normalizeKey(field.getKey());
      String endpointSuffix = endpointBindingSuffix(normalizedKey);
      JsonNode value = field.getValue();
      if (endpointSuffix != null && value.isValueNode()
          && !containsRestorableMarker(value, field.getKey(), false)) {
        String prefix = normalizedKey.substring(0, normalizedKey.length() - endpointSuffix.length());
        endpoints.put(prefix + ':' + normalizedKey, stableIdentityValue(value));
      }
    }
    return endpoints;
  }

  private static Map<String, String> endpointBindingsForPrefix(Map<String, String> bindings, String prefix) {
    Map<String, String> result = new HashMap<>();
    String mapPrefix = prefix + ':';
    for (Map.Entry<String, String> binding : bindings.entrySet()) {
      if (binding.getKey().startsWith(mapPrefix)) {
        result.put(binding.getKey(), binding.getValue());
      }
    }
    return result;
  }

  @Nullable
  private static String endpointBindingSuffix(String normalizedKey) {
    return matchingSuffix(normalizedKey,
        List.of("bootstrapservers", "serviceurl", "baseurl", "endpoint", "server", "host", "url", "uri"));
  }

  @Nullable
  private static String sensitiveBindingSuffix(String normalizedKey) {
    return matchingSuffix(normalizedKey, List.of(
        "secretaccesskey", "sharedaccesskey", "accesskeyid", "accesskey", "secretkey", "privatekey",
        "accountkey", "storagekey", "clientkey", "hmackey", "encryptionkey", "signingkey", "apikey",
        "passphrases", "passphrase", "passwords", "password", "credentials", "credential", "secrets",
        "secret", "tokens", "token", "passwd", "pwd", "signature", "authorization", "authheader", "auth"));
  }

  private static Map<String, JsonNode> normalizedFields(ObjectNode node) {
    Map<String, JsonNode> fields = new HashMap<>();
    for (Map.Entry<String, JsonNode> field : node.properties()) {
      fields.put(normalizeKey(field.getKey()), field.getValue());
    }
    return fields;
  }

  private static void validateCredentialPair(Map<String, JsonNode> submitted, Map<String, JsonNode> stored,
      List<String> identityKeys, List<String> secretKeys) {
    for (Map.Entry<String, JsonNode> secret : submitted.entrySet()) {
      String secretSuffix = matchingSuffix(secret.getKey(), secretKeys);
      if (secretSuffix == null || !containsRestorableMarker(secret.getValue(), null, true)) {
        continue;
      }
      String prefix = secret.getKey().substring(0, secret.getKey().length() - secretSuffix.length());
      JsonNode submittedIdentity = firstFieldWithPrefix(submitted, identityKeys, prefix);
      JsonNode storedIdentity = firstFieldWithPrefix(stored, identityKeys, prefix);
      if (!Objects.equals(submittedIdentity, storedIdentity)) {
        throw unresolvedMarker();
      }
    }
  }

  @Nullable
  private static JsonNode firstFieldWithPrefix(Map<String, JsonNode> fields, List<String> keys, String prefix) {
    for (Map.Entry<String, JsonNode> field : fields.entrySet()) {
      String suffix = matchingSuffix(field.getKey(), keys);
      if (suffix != null && field.getKey().substring(0, field.getKey().length() - suffix.length()).equals(prefix)) {
        return field.getValue();
      }
    }
    return null;
  }

  @Nullable
  private static String matchingSuffix(String normalizedKey, List<String> suffixes) {
    for (String suffix : suffixes) {
      if (normalizedKey.equals(suffix) || normalizedKey.endsWith(suffix)) {
        return suffix;
      }
    }
    return null;
  }

  private static String stableIdentityValue(JsonNode node) {
    if (node.isTextual() && URI_REFERENCE_PATTERN.matcher(node.textValue()).lookingAt()) {
      return uriIdentity(node.textValue());
    }
    return node.toString();
  }

  private static boolean containsRestorableMarker(JsonNode node, @Nullable String fieldName,
      boolean parentSensitive) {
    boolean sensitive = parentSensitive || isSensitiveKey(fieldName);
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> field : node.properties()) {
        if (containsRestorableMarker(field.getValue(), field.getKey(), sensitive)) {
          return true;
        }
      }
      return false;
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        if (containsRestorableMarker(element, fieldName, sensitive)) {
          return true;
        }
      }
      return false;
    }
    if (!node.isTextual() || isPlaceholder(node.textValue()) || !node.textValue().contains(REDACTION_MARKER)) {
      return false;
    }
    return sensitive || containsCredentialMarker(node.textValue());
  }

  private static IllegalArgumentException unresolvedMarker() {
    return new IllegalArgumentException("Redacted credential has no unambiguous unchanged stored value");
  }

  private static String redactText(@Nullable String fieldName, String value, boolean sensitive) {
    if (isPlaceholder(value)) {
      return value;
    }
    if (sensitive || isSensitiveKey(fieldName)) {
      return REDACTION_MARKER;
    }
    return redactStructuredCredentials(value);
  }

  private static boolean isPlaceholder(String value) {
    if (value.length() < 4 || !value.startsWith("${")) {
      return false;
    }

    int keyEnd = value.indexOf(':', 2);
    if (keyEnd < 0) {
      keyEnd = value.length() - 1;
    }
    if (keyEnd == 2) {
      return false;
    }
    for (int i = 2; i < keyEnd; i++) {
      char current = value.charAt(i);
      if (Character.isWhitespace(current) || current == '{' || current == '}') {
        return false;
      }
    }
    return value.endsWith("}");
  }

  private static boolean isSensitiveKey(@Nullable String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String normalized = normalizeKey(fieldName);
    if (normalized.isEmpty() || normalized.equals("usekeytab")) {
      return false;
    }
    return normalized.endsWith("password") || normalized.endsWith("passwords")
        || normalized.endsWith("passwd") || normalized.endsWith("pwd")
        || normalized.endsWith("passphrase") || normalized.endsWith("passphrases")
        || normalized.endsWith("secret") || normalized.endsWith("secrets")
        || normalized.endsWith("token") || normalized.endsWith("tokens")
        || normalized.endsWith("credential") || normalized.endsWith("credentials")
        || normalized.endsWith("accesskey") || normalized.endsWith("accesskeyid")
        || normalized.endsWith("apikey") || normalized.endsWith("secretkey")
        || normalized.endsWith("privatekey") || normalized.endsWith("accountkey")
        || normalized.endsWith("storagekey") || normalized.endsWith("sharedkey")
        || normalized.endsWith("masterkey") || normalized.endsWith("clientkey")
        || normalized.endsWith("hmackey") || normalized.endsWith("encryptionkey")
        || normalized.endsWith("signingkey") || normalized.endsWith("keytab")
        || normalized.equals("sig") || normalized.endsWith("signature")
        || normalized.equals("auth")
        || normalized.endsWith("authorization") || normalized.endsWith("authheader")
        || normalized.endsWith("serviceaccountjson") || normalized.endsWith("credentialjson")
        || normalized.equals("gcpkey") || normalized.equals("jsonkey")
        || normalized.endsWith("sslkeypem") || normalized.endsWith("sslkeystorekey")
        || normalized.contains("basicauthuserinfo")
        || normalized.contains("fsazureaccountkey")
        || normalized.contains("fsazureaccountoauth2clientsecret")
        || normalized.contains("fsazuresas");
  }

  private static boolean isSensitiveQueryKey(String key) {
    String decoded = decodeQueryKey(key);
    return decoded == null || isSensitiveQueryKeyLiteral(key)
        || !decoded.equals(key) && isSensitiveQueryKeyLiteral(decoded);
  }

  private static boolean isSensitiveQueryKeyLiteral(String key) {
    String normalized = normalizeKey(key);
    return isSensitiveKey(key) || normalized.equals("xamzcredential") || normalized.equals("xgoogcredential")
        || normalized.equals("auth") || normalized.equals("code") || normalized.equals("key")
        || normalized.equals("assertion") || normalized.equals("clientassertion") || normalized.equals("jwt")
        || normalized.equals("ticket") || normalized.equals("samlresponse") || normalized.equals("session")
        || normalized.equals("sessionid") || normalized.equals("sessionkey") || normalized.equals("sid");
  }

  @Nullable
  private static String decodeQueryKey(String key) {
    try {
      return URLDecoder.decode(key, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String normalizeQueryKey(String key) {
    String decoded = decodeQueryKey(key);
    return normalizeKey(decoded != null ? decoded : key);
  }

  private static String normalizeKey(String key) {
    return key.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
  }

  private static String redactStructuredCredentials(String value) {
    JsonNode structuredJson = tryParseStructuredJson(value);
    if (structuredJson != null) {
      JsonNode redactedJson = redactNode(structuredJson, null, false);
      return redactedJson.equals(structuredJson) ? value : redactedJson.toString();
    }
    List<UriSpan> uriSpans = findUriSpans(value);
    StringBuilder result = new StringBuilder(value.length());
    int offset = 0;
    for (UriSpan span : uriSpans) {
      result.append(redactCredentialAssignments(value.substring(offset, span._start)));
      result.append(redactUri(value.substring(span._start, span._end)));
      offset = span._end;
    }
    result.append(redactCredentialAssignments(value.substring(offset)));
    return result.toString();
  }

  private static String redactCredentialAssignments(String value) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(value);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      if (!isSensitiveKey(matcher.group(2))) {
        continue;
      }
      String assignmentValue = matcher.group(3);
      String unquotedValue = unquote(assignmentValue);
      if (isPlaceholder(unquotedValue)) {
        continue;
      }
      String replacementValue = quoteLike(assignmentValue, REDACTION_MARKER);
      matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(1) + replacementValue));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static boolean containsCredentialMarker(String value) {
    if (!value.contains(REDACTION_MARKER)) {
      return false;
    }
    JsonNode structuredJson = tryParseStructuredJson(value);
    if (structuredJson != null) {
      return containsRestorableMarker(structuredJson, null, false);
    }
    List<UriSpan> uriSpans = findUriSpans(value);
    int offset = 0;
    for (UriSpan span : uriSpans) {
      if (assignmentSegmentContainsCredentialMarker(value.substring(offset, span._start))
          || uriContainsCredentialMarker(value.substring(span._start, span._end))) {
        return true;
      }
      offset = span._end;
    }
    return assignmentSegmentContainsCredentialMarker(value.substring(offset));
  }

  private static boolean assignmentSegmentContainsCredentialMarker(String value) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(value);
    while (matcher.find()) {
      String assignmentValue = unquote(matcher.group(3));
      if (isSensitiveKey(matcher.group(2)) && !isPlaceholder(assignmentValue)
          && assignmentValue.contains(REDACTION_MARKER)) {
        return true;
      }
    }
    return false;
  }

  private static String restoreStructuredCredentials(String submitted, String stored) {
    JsonNode submittedJson = tryParseStructuredJson(submitted);
    JsonNode storedJson = tryParseStructuredJson(stored);
    if (submittedJson != null || storedJson != null) {
      if (submittedJson == null || storedJson == null || submittedJson.isObject() != storedJson.isObject()) {
        throw unresolvedMarker();
      }
      return restoreNode(submittedJson, storedJson, null, false).toString();
    }
    String redactedStored = redactStructuredCredentials(stored);
    if (!structuredSecurityIdentity(submitted).equals(structuredSecurityIdentity(redactedStored))) {
      throw unresolvedMarker();
    }
    validateStructuredCredentialPairs(submitted, redactedStored);
    String restoredUris = restoreUriCredentials(submitted, stored);
    return restoreCredentialAssignments(restoredUris, stored);
  }

  private static String structuredSecurityIdentity(String value) {
    StringBuilder identity = new StringBuilder();
    Matcher moduleMatcher = JAAS_MODULE_PATTERN.matcher(value);
    while (moduleMatcher.find()) {
      identity.append("module:").append(moduleMatcher.group(1)).append(';');
    }
    Matcher assignmentMatcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(value);
    while (assignmentMatcher.find()) {
      String key = assignmentMatcher.group(2);
      if (!isSecurityIdentityAssignmentKey(key)) {
        continue;
      }
      String assignmentValue = unquote(assignmentMatcher.group(3));
      identity.append(normalizeKey(key)).append(':').append(securityIdentityValue(assignmentValue)).append(';');
    }
    return identity.toString();
  }

  private static boolean isSecurityIdentityAssignmentKey(String key) {
    String normalized = normalizeKey(key);
    return normalized.equals("user") || normalized.equals("username") || normalized.equals("principal")
        || normalized.equals("accountname") || normalized.endsWith("clientid") || normalized.endsWith("applicationid")
        || normalized.endsWith("accesskeyid") || normalized.endsWith("sharedaccesskeyname")
        || normalized.endsWith("sharedaccesspolicyname") || normalized.endsWith("accesskeyname")
        || normalized.endsWith("endpoint") || normalized.endsWith("host") || normalized.endsWith("server")
        || normalized.endsWith("url") || normalized.endsWith("uri")
        || isExecutableClassSelectorKey(normalized);
  }

  private static void validateStructuredCredentialPairs(String submitted, String redactedStored) {
    Map<String, List<String>> submittedValues = collectAssignmentValues(submitted);
    Map<String, List<String>> storedValues = collectAssignmentValues(redactedStored);
    validateStructuredCredentialPair(submittedValues, storedValues,
        List.of("username", "user", "principal", "accountname"),
        List.of("passwords", "password", "passwd", "pwd", "passphrases", "passphrase"));
  }

  private static Map<String, List<String>> collectAssignmentValues(String value) {
    Map<String, List<String>> result = new HashMap<>();
    List<UriSpan> uriSpans = findUriSpans(value);
    int offset = 0;
    for (UriSpan span : uriSpans) {
      collectAllAssignmentValues(value.substring(offset, span._start), result);
      offset = span._end;
    }
    collectAllAssignmentValues(value.substring(offset), result);
    return result;
  }

  private static void collectAllAssignmentValues(String segment, Map<String, List<String>> values) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(segment);
    while (matcher.find()) {
      values.computeIfAbsent(normalizeKey(matcher.group(2)), ignored -> new ArrayList<>())
          .add(unquote(matcher.group(3)));
    }
  }

  private static void validateStructuredCredentialPair(Map<String, List<String>> submitted,
      Map<String, List<String>> stored, List<String> identityKeys, List<String> secretKeys) {
    for (Map.Entry<String, List<String>> secret : submitted.entrySet()) {
      String secretSuffix = matchingSuffix(secret.getKey(), secretKeys);
      if (secretSuffix == null || secret.getValue().stream().noneMatch(TableConfigRedactionUtils::markerValue)) {
        continue;
      }
      String prefix = secret.getKey().substring(0, secret.getKey().length() - secretSuffix.length());
      if (!Objects.equals(uniqueAssignmentIdentity(submitted, identityKeys, prefix),
          uniqueAssignmentIdentity(stored, identityKeys, prefix))) {
        throw unresolvedMarker();
      }
    }
  }

  private static boolean markerValue(String value) {
    return !isPlaceholder(value) && value.contains(REDACTION_MARKER);
  }

  @Nullable
  private static String uniqueAssignmentIdentity(Map<String, List<String>> values, List<String> keys,
      String prefix) {
    String identity = null;
    for (Map.Entry<String, List<String>> field : values.entrySet()) {
      String suffix = matchingSuffix(field.getKey(), keys);
      if (suffix == null
          || !field.getKey().substring(0, field.getKey().length() - suffix.length()).equals(prefix)) {
        continue;
      }
      for (String value : field.getValue()) {
        if (identity != null && !identity.equals(value)) {
          throw unresolvedMarker();
        }
        identity = value;
      }
    }
    return identity;
  }

  private static String securityIdentityValue(String value) {
    Matcher uriMatcher = URI_REFERENCE_PATTERN.matcher(value);
    return uriMatcher.lookingAt() ? uriIdentity(value) : value;
  }

  private static String restoreUriCredentials(String submitted, String stored) {
    List<UriSpan> submittedSpans = findUriSpans(submitted);
    List<UriSpan> storedSpans = findUriSpans(stored);
    boolean[] usedStoredUris = new boolean[storedSpans.size()];
    StringBuilder result = new StringBuilder(submitted.length());
    int offset = 0;
    for (UriSpan submittedSpan : submittedSpans) {
      String submittedUri = submitted.substring(submittedSpan._start, submittedSpan._end);
      result.append(submitted, offset, submittedSpan._start);
      if (!uriContainsCredentialMarker(submittedUri)) {
        result.append(submittedUri);
      } else {
        int match = findStoredUri(submittedUri, stored, storedSpans, usedStoredUris);
        if (match < 0) {
          throw unresolvedMarker();
        }
        UriSpan storedSpan = storedSpans.get(match);
        String storedUri = stored.substring(storedSpan._start, storedSpan._end);
        result.append(restoreUri(submittedUri, storedUri));
        usedStoredUris[match] = true;
      }
      offset = submittedSpan._end;
    }
    result.append(submitted, offset, submitted.length());
    return result.toString();
  }

  private static int findStoredUri(String submittedUri, String stored, List<UriSpan> storedSpans,
      boolean[] usedStoredUris) {
    String submittedIdentity = uriIdentity(submittedUri);
    int match = -1;
    for (int i = 0; i < storedSpans.size(); i++) {
      if (usedStoredUris[i]) {
        continue;
      }
      UriSpan span = storedSpans.get(i);
      String candidate = stored.substring(span._start, span._end);
      if (submittedIdentity.equals(uriIdentity(redactUri(candidate)))) {
        if (match >= 0) {
          return -1;
        }
        match = i;
      }
    }
    return match;
  }

  private static String restoreCredentialAssignments(String submitted, String stored) {
    Map<String, List<String>> storedValues = collectCredentialAssignmentValues(stored);
    List<UriSpan> uriSpans = findUriSpans(submitted);
    StringBuilder result = new StringBuilder(submitted.length());
    int offset = 0;
    for (UriSpan span : uriSpans) {
      result.append(restoreCredentialAssignmentsInSegment(submitted.substring(offset, span._start), storedValues));
      result.append(submitted, span._start, span._end);
      offset = span._end;
    }
    result.append(restoreCredentialAssignmentsInSegment(submitted.substring(offset), storedValues));
    return result.toString();
  }

  private static Map<String, List<String>> collectCredentialAssignmentValues(String value) {
    Map<String, List<String>> result = new HashMap<>();
    List<UriSpan> uriSpans = findUriSpans(value);
    int offset = 0;
    for (UriSpan span : uriSpans) {
      collectCredentialAssignmentValuesFromSegment(value.substring(offset, span._start), result);
      offset = span._end;
    }
    collectCredentialAssignmentValuesFromSegment(value.substring(offset), result);
    return result;
  }

  private static void collectCredentialAssignmentValuesFromSegment(String segment,
      Map<String, List<String>> values) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(segment);
    while (matcher.find()) {
      if (isSensitiveKey(matcher.group(2))) {
        values.computeIfAbsent(normalizeKey(matcher.group(2)), ignored -> new ArrayList<>()).add(matcher.group(3));
      }
    }
  }

  private static String restoreCredentialAssignmentsInSegment(String segment,
      Map<String, List<String>> storedValues) {
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(segment);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      if (!isSensitiveKey(matcher.group(2))) {
        continue;
      }
      String submittedValue = unquote(matcher.group(3));
      if (isPlaceholder(submittedValue)) {
        continue;
      }
      if (REDACTION_MARKER.equals(submittedValue)) {
        String storedValue = uniqueStoredValue(storedValues.get(normalizeKey(matcher.group(2))));
        matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(1) + storedValue));
      } else if (submittedValue.contains(REDACTION_MARKER)) {
        throw unresolvedMarker();
      }
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String uniqueStoredValue(@Nullable List<String> storedValues) {
    if (storedValues == null || storedValues.isEmpty()) {
      throw unresolvedMarker();
    }
    String value = storedValues.get(0);
    for (int i = 1; i < storedValues.size(); i++) {
      if (!value.equals(storedValues.get(i))) {
        throw unresolvedMarker();
      }
    }
    return value;
  }

  private static String unquote(String value) {
    if (value.length() >= 2) {
      char first = value.charAt(0);
      if ((first == '\'' || first == '"') && value.charAt(value.length() - 1) == first) {
        return value.substring(1, value.length() - 1);
      }
    }
    return value;
  }

  private static String quoteLike(String original, String replacement) {
    if (original.length() >= 2) {
      char first = original.charAt(0);
      if ((first == '\'' || first == '"') && original.charAt(original.length() - 1) == first) {
        return first + replacement + first;
      }
    }
    return replacement;
  }

  private static List<UriSpan> findUriSpans(String value) {
    List<UriSpan> spans = new ArrayList<>();
    Matcher matcher = URI_REFERENCE_PATTERN.matcher(value);
    int searchFrom = 0;
    while (searchFrom < value.length() && matcher.find(searchFrom)) {
      int start = matcher.start();
      int end = findUriEnd(value, matcher.end());
      spans.add(new UriSpan(start, end));
      searchFrom = Math.max(end, matcher.end());
    }
    return spans;
  }

  private static int findUriEnd(String value, int from) {
    int placeholderDepth = 0;
    boolean inAuthority = true;
    for (int i = from; i < value.length(); i++) {
      char c = value.charAt(i);
      if (startsPlaceholder(value, i)) {
        placeholderDepth++;
        continue;
      }
      if (placeholderDepth > 0) {
        if (c == '{' && value.charAt(i - 1) != '$') {
          placeholderDepth++;
        } else if (c == '}') {
          placeholderDepth--;
        }
        continue;
      }
      if (startsWithUriReference(value, i)) {
        return i;
      }
      if (Character.isWhitespace(c) || ((c == '\'' || c == '"') && !isEscaped(value, i))) {
        return i;
      }
      if (c == '/' || c == '?' || c == '#') {
        inAuthority = false;
      }
      if ((c == ',' || c == ';')
          && !(inAuthority && hasUserInfoTerminatorAhead(value, i + 1))
          && (startsWithUriScheme(value, i + 1) || startsWithAssignment(value, i + 1))) {
        return i;
      }
    }
    return value.length();
  }

  private static boolean hasUserInfoTerminatorAhead(String value, int from) {
    int placeholderDepth = 0;
    for (int i = from; i < value.length(); i++) {
      char c = value.charAt(i);
      if (startsPlaceholder(value, i)) {
        placeholderDepth++;
        continue;
      }
      if (placeholderDepth > 0) {
        if (c == '{' && value.charAt(i - 1) != '$') {
          placeholderDepth++;
        } else if (c == '}') {
          placeholderDepth--;
        }
        continue;
      }
      if (c == '@') {
        return true;
      }
      if (c == '/' || c == '?' || c == '#' || Character.isWhitespace(c)
          || (c == '\'' || c == '"') && !isEscaped(value, i)) {
        return false;
      }
    }
    return false;
  }

  private static boolean startsWithUriScheme(String value, int from) {
    int index = from;
    while (index < value.length() && Character.isWhitespace(value.charAt(index))) {
      index++;
    }
    Matcher matcher = URI_REFERENCE_PATTERN.matcher(value);
    matcher.region(index, value.length());
    return matcher.lookingAt();
  }

  private static boolean startsWithUriReference(String value, int from) {
    Matcher matcher = URI_REFERENCE_PATTERN.matcher(value);
    matcher.region(from, value.length());
    return matcher.lookingAt();
  }

  private static boolean startsWithAssignment(String value, int from) {
    int index = from;
    while (index < value.length() && Character.isWhitespace(value.charAt(index))) {
      index++;
    }
    Matcher matcher = CREDENTIAL_ASSIGNMENT_PATTERN.matcher(value);
    matcher.region(index, value.length());
    return matcher.lookingAt();
  }

  private static boolean startsPlaceholder(String value, int index) {
    return value.charAt(index) == '$' && index + 1 < value.length() && value.charAt(index + 1) == '{';
  }

  private static boolean isEscaped(String value, int index) {
    int slashCount = 0;
    for (int i = index - 1; i >= 0 && value.charAt(i) == '\\'; i--) {
      slashCount++;
    }
    return slashCount % 2 != 0;
  }

  private static String redactUri(String uri) {
    UriParts parts = new UriParts(uri);
    return parts.rebuild(redactUserInfo(parts._userInfo), redactQuery(parts._query), redactFragment(parts._fragment));
  }

  private static String restoreUri(String submitted, String stored) {
    UriParts submittedParts = new UriParts(submitted);
    UriParts storedParts = new UriParts(stored);
    String userInfo = restoreUserInfo(submittedParts._userInfo, storedParts._userInfo);
    String query = restoreQuery(submittedParts._query, storedParts._query);
    String fragment = restoreFragment(submittedParts._fragment, storedParts._fragment);
    return submittedParts.rebuild(userInfo, query, fragment);
  }

  private static String uriIdentity(String uri) {
    UriParts parts = new UriParts(uri);
    return parts._prefix + nonCredentialUserInfo(parts._userInfo) + parts._resource
        + nonCredentialQuery(parts._query) + nonCredentialFragment(parts._fragment);
  }

  private static String nonCredentialUserInfo(@Nullable String userInfo) {
    if (userInfo == null) {
      return "";
    }
    List<String> parts = splitOutsidePlaceholders(userInfo, ':');
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      if (!REDACTION_MARKER.equals(part)) {
        return "userinfo=" + part + "@";
      }
    }
    return "";
  }

  private static String nonCredentialQuery(@Nullable String query) {
    if (query == null) {
      return "";
    }
    StringBuilder identity = new StringBuilder();
    for (String parameter : splitQuery(query)) {
      int equalsIndex = parameter.indexOf('=');
      String key = equalsIndex >= 0 ? stripQueryPrefix(parameter.substring(0, equalsIndex)) : parameter;
      String value = equalsIndex >= 0 ? parameter.substring(equalsIndex + 1) : "";
      String decodedKey = decodeQueryKey(key);
      String identityKey = decodedKey != null ? decodedKey : key;
      String normalizedIdentityKey = normalizeKey(identityKey);
      if (isSecurityIdentityAssignmentKey(identityKey)
          && (!isSensitiveQueryKey(key) || normalizedIdentityKey.endsWith("accesskeyid"))
          && !queryValueContainsCredentialMarker(value)) {
        identity.append('&').append(normalizedIdentityKey).append('=').append(value);
      }
    }
    return identity.toString();
  }

  private static boolean uriContainsCredentialMarker(String uri) {
    UriParts parts = new UriParts(uri);
    if (parts._userInfo != null) {
      for (String part : splitOutsidePlaceholders(parts._userInfo, ':')) {
        if (!isPlaceholder(part) && part.contains(REDACTION_MARKER)) {
          return true;
        }
      }
    }
    return queryContainsCredentialMarker(parts._query)
        || queryContainsCredentialMarker(parameterLikeFragment(parts._fragment));
  }

  private static String redactFragment(String fragment) {
    String parameters = parameterLikeFragment(fragment);
    return parameters != null ? transformQuery(parameters, null) : fragment;
  }

  private static String restoreFragment(String submitted, String stored) {
    String submittedParameters = parameterLikeFragment(submitted);
    if (submittedParameters == null || !queryContainsCredentialMarker(submittedParameters)) {
      return submitted;
    }
    String storedParameters = parameterLikeFragment(stored);
    return transformQuery(submittedParameters, collectQueryValues(storedParameters));
  }

  private static String nonCredentialFragment(String fragment) {
    String parameters = parameterLikeFragment(fragment);
    return parameters != null ? nonCredentialQuery(parameters) : fragment;
  }

  @Nullable
  private static String parameterLikeFragment(String fragment) {
    return fragment.indexOf('=') >= 0 ? fragment : null;
  }

  @Nullable
  private static String redactUserInfo(@Nullable String userInfo) {
    if (userInfo == null) {
      return null;
    }
    List<String> parts = splitOutsidePlaceholders(userInfo, ':');
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      if (!part.isEmpty() && !isPlaceholder(part)) {
        parts.set(i, REDACTION_MARKER);
      }
    }
    return String.join(":", parts);
  }

  @Nullable
  private static String restoreUserInfo(@Nullable String submitted, @Nullable String stored) {
    if (submitted == null || !submitted.contains(REDACTION_MARKER)) {
      return submitted;
    }
    if (stored == null) {
      throw unresolvedMarker();
    }
    List<String> submittedParts = splitOutsidePlaceholders(submitted, ':');
    List<String> storedParts = splitOutsidePlaceholders(stored, ':');
    if (submittedParts.size() != storedParts.size()) {
      throw unresolvedMarker();
    }
    for (int i = 0; i < submittedParts.size(); i++) {
      String part = submittedParts.get(i);
      if (isPlaceholder(part)) {
        continue;
      } else if (REDACTION_MARKER.equals(part)) {
        submittedParts.set(i, storedParts.get(i));
      } else if (part.contains(REDACTION_MARKER)) {
        throw unresolvedMarker();
      }
    }
    return String.join(":", submittedParts);
  }

  @Nullable
  private static String redactQuery(@Nullable String query) {
    if (query == null || query.length() <= 1) {
      return query;
    }
    return transformQuery(query, null);
  }

  @Nullable
  private static String restoreQuery(@Nullable String submitted, @Nullable String stored) {
    if (submitted == null || !submitted.contains(REDACTION_MARKER)) {
      return submitted;
    }
    Map<String, List<String>> storedValues = collectQueryValues(stored);
    return transformQuery(submitted, storedValues);
  }

  private static String transformQuery(String query, @Nullable Map<String, List<String>> storedValues) {
    StringBuilder result = new StringBuilder(query.length());
    int start = 0;
    int placeholderDepth = 0;
    for (int i = 0; i <= query.length(); i++) {
      if (i < query.length()) {
        char c = query.charAt(i);
        if (startsPlaceholder(query, i)) {
          placeholderDepth++;
          continue;
        }
        if (placeholderDepth > 0) {
          if (c == '{' && query.charAt(i - 1) != '$') {
            placeholderDepth++;
          } else if (c == '}') {
            placeholderDepth--;
          }
          continue;
        }
        if (c != '&' && c != ';') {
          continue;
        }
      }
      result.append(transformQueryParameter(query.substring(start, i), storedValues));
      if (i < query.length()) {
        result.append(query.charAt(i));
      }
      start = i + 1;
    }
    return result.toString();
  }

  private static String transformQueryParameter(String parameter,
      @Nullable Map<String, List<String>> storedValues) {
    int equalsIndex = parameter.indexOf('=');
    if (equalsIndex < 0) {
      return parameter;
    }
    String key = stripQueryPrefix(parameter.substring(0, equalsIndex));
    String keyPrefix = parameter.substring(0, equalsIndex + 1);
    String value = parameter.substring(equalsIndex + 1);
    if (isSensitiveQueryKey(key)) {
      if (storedValues == null) {
        return keyPrefix + (isPlaceholder(value) ? value : REDACTION_MARKER);
      }
      if (isPlaceholder(value)) {
        return parameter;
      }
      if (REDACTION_MARKER.equals(value)) {
        return keyPrefix + uniqueStoredValue(storedValues.get(normalizeQueryKey(key)));
      }
      if (value.contains(REDACTION_MARKER)) {
        throw unresolvedMarker();
      }
      return parameter;
    }
    if (storedValues == null) {
      return keyPrefix + redactQueryValue(value);
    }
    if (queryValueContainsCredentialMarker(value)) {
      String storedValue = uniqueStoredValue(storedValues.get(normalizeQueryKey(key)));
      return keyPrefix + restoreQueryValue(value, storedValue);
    }
    return parameter;
  }

  private static String redactQueryValue(String value) {
    String redacted = redactStructuredCredentials(value);
    if (!redacted.equals(value)) {
      return redacted;
    }
    String decoded = decodeQueryValue(value);
    if (decoded == null || decoded.equals(value)) {
      return value;
    }
    String redactedDecoded = redactStructuredCredentials(decoded);
    return redactedDecoded.equals(decoded) ? value : encodeQueryValue(redactedDecoded);
  }

  private static String restoreQueryValue(String submitted, String stored) {
    if (containsCredentialMarker(submitted)) {
      return restoreStructuredCredentials(submitted, stored);
    }
    String decodedSubmitted = decodeQueryValue(submitted);
    String decodedStored = decodeQueryValue(stored);
    if (decodedSubmitted == null || decodedStored == null || !containsCredentialMarker(decodedSubmitted)) {
      throw unresolvedMarker();
    }
    return encodeQueryValue(restoreStructuredCredentials(decodedSubmitted, decodedStored));
  }

  private static boolean queryValueContainsCredentialMarker(String value) {
    if (isPlaceholder(value)) {
      return false;
    }
    if (containsCredentialMarker(value)) {
      return true;
    }
    String decoded = decodeQueryValue(value);
    return decoded != null && !decoded.equals(value) && containsCredentialMarker(decoded);
  }

  @Nullable
  private static String decodeQueryValue(String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String encodeQueryValue(String value) {
    String encodedMarker = URLEncoder.encode(REDACTION_MARKER, StandardCharsets.UTF_8);
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
        .replace(encodedMarker, REDACTION_MARKER);
  }

  private static String stripQueryPrefix(String key) {
    return key.startsWith("?") ? key.substring(1) : key;
  }

  private static Map<String, List<String>> collectQueryValues(@Nullable String query) {
    Map<String, List<String>> result = new HashMap<>();
    if (query == null) {
      return result;
    }
    for (String parameter : splitQuery(query)) {
      int equalsIndex = parameter.indexOf('=');
      if (equalsIndex < 0) {
        continue;
      }
      String key = stripQueryPrefix(parameter.substring(0, equalsIndex));
      result.computeIfAbsent(normalizeQueryKey(key), ignored -> new ArrayList<>())
          .add(parameter.substring(equalsIndex + 1));
    }
    return result;
  }

  private static boolean queryContainsCredentialMarker(@Nullable String query) {
    if (query == null || !query.contains(REDACTION_MARKER)) {
      return false;
    }
    for (String parameter : splitQuery(query)) {
      int equalsIndex = parameter.indexOf('=');
      if (equalsIndex < 0) {
        continue;
      }
      String key = stripQueryPrefix(parameter.substring(0, equalsIndex));
      String value = parameter.substring(equalsIndex + 1);
      if ((isSensitiveQueryKey(key) && !isPlaceholder(value) && value.contains(REDACTION_MARKER))
          || !isSensitiveQueryKey(key) && queryValueContainsCredentialMarker(value)) {
        return true;
      }
    }
    return false;
  }

  private static List<String> splitQuery(String query) {
    List<String> result = new ArrayList<>();
    int start = 0;
    int placeholderDepth = 0;
    for (int i = 0; i <= query.length(); i++) {
      if (i < query.length()) {
        char c = query.charAt(i);
        if (startsPlaceholder(query, i)) {
          placeholderDepth++;
          continue;
        }
        if (placeholderDepth > 0) {
          if (c == '{' && query.charAt(i - 1) != '$') {
            placeholderDepth++;
          } else if (c == '}') {
            placeholderDepth--;
          }
          continue;
        }
        if (c != '&' && c != ';') {
          continue;
        }
      }
      result.add(query.substring(start, i));
      start = i + 1;
    }
    return result;
  }

  private static List<String> splitOutsidePlaceholders(String value, char delimiter) {
    List<String> result = new ArrayList<>();
    int start = 0;
    int placeholderDepth = 0;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (startsPlaceholder(value, i)) {
        placeholderDepth++;
        continue;
      }
      if (placeholderDepth > 0) {
        if (c == '{' && value.charAt(i - 1) != '$') {
          placeholderDepth++;
        } else if (c == '}') {
          placeholderDepth--;
        }
      } else if (c == delimiter) {
        result.add(value.substring(start, i));
        start = i + 1;
      }
    }
    result.add(value.substring(start));
    return result;
  }

  private static int findOutsidePlaceholder(String value, char target, int from, int to) {
    int placeholderDepth = 0;
    for (int i = from; i < to; i++) {
      char c = value.charAt(i);
      if (startsPlaceholder(value, i)) {
        placeholderDepth++;
        continue;
      }
      if (placeholderDepth > 0) {
        if (c == '{' && value.charAt(i - 1) != '$') {
          placeholderDepth++;
        } else if (c == '}') {
          placeholderDepth--;
        }
      } else if (c == target) {
        return i;
      }
    }
    return -1;
  }

  private static int findLastOutsidePlaceholder(String value, char target, int from, int to) {
    int result = -1;
    int placeholderDepth = 0;
    for (int i = from; i < to; i++) {
      char c = value.charAt(i);
      if (startsPlaceholder(value, i)) {
        placeholderDepth++;
        continue;
      }
      if (placeholderDepth > 0) {
        if (c == '{' && value.charAt(i - 1) != '$') {
          placeholderDepth++;
        } else if (c == '}') {
          placeholderDepth--;
        }
      } else if (c == target) {
        result = i;
      }
    }
    return result;
  }

  @Nullable
  private static JsonNode tryParseStructuredJson(String value) {
    String trimmed = value.trim();
    if (trimmed.length() < 2
        || !(trimmed.startsWith("{") && trimmed.endsWith("}")
        || trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      return null;
    }
    try {
      JsonNode node = JsonUtils.stringToJsonNode(trimmed);
      return node != null && (node.isObject() || node.isArray()) ? node : null;
    } catch (IOException e) {
      return null;
    }
  }

  private static TableConfig toTableConfig(JsonNode node, String errorMessage) {
    try {
      return JsonUtils.jsonNodeToObject(node, TableConfig.class);
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException(errorMessage, e);
    }
  }

  private static final class UriSpan {
    private final int _start;
    private final int _end;

    private UriSpan(int start, int end) {
      _start = start;
      _end = end;
    }
  }

  private static final class UriParts {
    private final String _prefix;
    private final String _resource;
    private final String _fragment;
    @Nullable
    private final String _userInfo;
    @Nullable
    private final String _query;

    private UriParts(String uri) {
      int schemeEnd = uri.indexOf("://") + 3;
      int queryStart = findOutsidePlaceholder(uri, '?', schemeEnd, uri.length());
      int fragmentStart = findOutsidePlaceholder(uri, '#', schemeEnd, uri.length());
      if (queryStart >= 0 && fragmentStart >= 0 && queryStart > fragmentStart) {
        queryStart = -1;
      }
      int resourceEnd = uri.length();
      if (queryStart >= 0) {
        resourceEnd = queryStart;
      }
      if (fragmentStart >= 0 && fragmentStart < resourceEnd) {
        resourceEnd = fragmentStart;
      }
      int authorityEnd = findOutsidePlaceholder(uri, '/', schemeEnd, resourceEnd);
      if (authorityEnd < 0) {
        authorityEnd = resourceEnd;
      }
      int userInfoEnd = findLastOutsidePlaceholder(uri, '@', schemeEnd, authorityEnd);
      int resourceStart = userInfoEnd >= 0 ? userInfoEnd + 1 : schemeEnd;

      _prefix = uri.substring(0, schemeEnd);
      _userInfo = userInfoEnd >= 0 ? uri.substring(schemeEnd, userInfoEnd) : null;
      _resource = uri.substring(resourceStart, resourceEnd);
      int queryEnd = fragmentStart >= 0 ? fragmentStart : uri.length();
      _query = queryStart >= 0 ? uri.substring(queryStart, queryEnd) : null;
      _fragment = fragmentStart >= 0 ? uri.substring(fragmentStart) : "";
    }

    private String rebuild(@Nullable String userInfo, @Nullable String query, String fragment) {
      return _prefix + (userInfo != null ? userInfo + "@" : "") + _resource
          + (query != null ? query : "") + fragment;
    }
  }
}
