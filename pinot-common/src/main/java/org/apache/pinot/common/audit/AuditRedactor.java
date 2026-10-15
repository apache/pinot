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
package org.apache.pinot.common.audit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Masks credential-bearing values out of captured audit payloads before they are serialized into an
/// [AuditEvent].
///
/// Audit records are written to stdout and to a rotating file, and are usually shipped onward to a log
/// pipeline whose operators are not entitled to the credentials a caller happens to send. Anything that
/// leaves this class must therefore be assumed to be readable by more people than the request itself was.
///
/// Three rules follow from that, and all are deliberate:
///
///   - **Redaction is unconditional.** There is no configuration that turns it off, because every
///     configuration that turns it off is a credential leak.
///   - **A body that cannot be parsed is reported by size only.** Secrets can only be located inside a
///     structure that is understood; in an opaque payload they cannot be found, so nothing of it is kept.
///   - **A body is parsed only if it is one complete JSON object or array with nothing after it.** See
///     [#parseWholeValue(String)]. A bare scalar has no key to judge it by, so it is reported by size.
///
/// Key matching errs towards over-redaction: masking a harmless `tokenUri` costs an audit reader
/// nothing, while missing one `accessKey` is the bug this class exists to prevent.
///
/// ## Known residual gap
///
/// Values are inspected only for URL userinfo (`scheme://user:password@host`), which is stripped. Any
/// other credential embedded in the *value* of an innocuously named key is not caught - a JDBC URL with a
/// `password=` query parameter, say, or a presigned URL whose signature sits inside the value. Callers
/// must not read this class as a guarantee that a captured payload is free of credentials.
///
/// This class is stateless and thread-safe.
final class AuditRedactor {

  static final String MASKED_VALUE = "*****";

  private static final Logger LOG = LoggerFactory.getLogger(AuditRedactor.class);

  /// Key patterns whose values are credential material. Applied case-insensitively against the full key,
  /// so dotted keys such as `input.fs.prop.accessKey` are matched by their suffix.
  ///
  /// This is a superset of [org.apache.pinot.spi.utils.Obfuscator]'s defaults, which anchor on a suffix
  /// and so miss the AWS `accessKey` / `accessKeyId` pair that motivated this class. Notable entries:
  ///
  ///   - `auth` is matched broadly rather than as `authorization`, to cover Confluent's
  ///     `basic.auth.user.info` - a literal `user:password` pair present in every schema-registry-backed
  ///     realtime table.
  ///   - Any key ending in `key` is sensitive (`fs.s3a.access.key`, `gcpKey`, `jsonKey`,
  ///     `stream.confluent.key`), as is a key naming a key's id, path, file or content (`sseKmsKeyId`,
  ///     `gcpKeyPath`, `keyFile`, `keyContentBase64`). Structural names that merely end in `key` are
  ///     exempted through [#SAFE_KEY_NAMES].
  private static final List<Pattern> SENSITIVE_KEY_PATTERNS =
      Stream.of(".*secret.*", ".*password.*", ".*passwd.*", ".*pwd$", ".*passphrase.*", ".*credential.*",
              ".*creds.*", ".*key$", ".*key[\\s._-]*(id|path|file|content).*", ".*access[\\s._-]*key.*",
              ".*api[\\s_-]*key.*", ".*private[\\s_-]*key.*", ".*encryption[\\s_-]*key.*",
              ".*signing[\\s_-]*key.*", ".*keytab.*", ".*token.*", ".*auth.*", ".*jaas.*config.*", ".*signature.*",
              ".*cookie.*", ".*keystore.*", ".*truststore.*", ".*user[\\s._-]*info$")
          .map(pattern -> Pattern.compile("(?i)" + pattern))
          .collect(Collectors.toList());

  /// Names that end in `key` without being a credential. Compared, lowercased, against both the full key
  /// and its last dotted segment.
  private static final Set<String> SAFE_KEY_NAMES =
      Set.of("primarykey", "partitionkey", "sortkey", "joinkey", "groupbykey", "dimensionkey", "metrickey",
          "timecolumnkey", "schemaregistrykey");

  /// Key suffixes that name an authentication *mode*, not a credential - a provider class name, or
  /// `URL` / `USER_INFO` for the schema registry. They tell an audit reader how a caller authenticated,
  /// so they survive even though `credential` and `auth` would otherwise match them.
  private static final Set<String> SAFE_KEY_SUFFIXES =
      Set.of(".credentials.source", ".credentials.provider", ".credentials.providers");

  /// A URL carrying credentials in its userinfo component. Group 1 is the scheme, group 2 the userinfo
  /// and group 3 the host onwards, which is kept for diagnostics.
  private static final Pattern URL_WITH_USERINFO =
      Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]*://)([^/@\\s]+)@(.*)$", Pattern.DOTALL);

  private AuditRedactor() {
  }

  /// Returns the request body with every credential-bearing value masked, or a size-only placeholder when
  /// the body is not one complete, parseable JSON value.
  ///
  /// @param body the raw captured body, possibly null or blank
  /// @return the redacted body, or null if there was nothing to record
  static String redactBody(String body) {
    if (StringUtils.isBlank(body)) {
      return null;
    }
    boolean truncated = body.endsWith(AuditRequestProcessor.TRUNCATION_MARKER);
    try {
      JsonNode parsed = parseWholeValue(body);
      if (parsed != null && parsed.isContainerNode()) {
        return JsonUtils.objectToString(redactNode(parsed));
      }
    } catch (Exception e) {
      // Never log the exception itself. Jackson embeds the offending token in its message, so a body
      // that is a bare credential would write that credential into the log this class protects.
      LOG.debug("Audit payload is not parseable JSON ({}); recording size only", e.getClass().getSimpleName());
    }
    return unparseableMarker(body, truncated);
  }

  /// Parses `body` only if it is a single complete JSON value with nothing following it.
  ///
  /// [com.fasterxml.jackson.databind.ObjectReader#readTree(String)] stops at the first value and
  /// silently discards whatever follows, because `FAIL_ON_TRAILING_TOKENS` is off by default. Relying on
  /// it would record a concatenated or truncated payload as a well-formed body with the remainder - and
  /// the truncation indicator itself - quietly dropped, leaving a reader unable to tell the record is
  /// incomplete. Reading through an explicit parser and then demanding the stream be exhausted rejects
  /// both cases, so they fall to the size-only path instead.
  ///
  /// @return the parsed value, or null if the body is empty or carries trailing content
  private static JsonNode parseWholeValue(String body)
      throws IOException {
    try (JsonParser parser = JsonUtils.DEFAULT_READER.createParser(body)) {
      JsonNode node = JsonUtils.DEFAULT_READER.readTree(parser);
      if (node == null || node.isMissingNode() || parser.nextToken() != null) {
        return null;
      }
      return node;
    }
  }

  /// Returns a copy of `map` with the values of credential-bearing keys masked. Multi-valued entries are
  /// replaced wholesale rather than element-wise, so the number of values is not disclosed either.
  static Map<String, Object> redact(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return map;
    }
    Map<String, Object> redacted = new HashMap<>(map.size());
    map.forEach((key, value) -> redacted.put(key, isSensitiveKey(key) ? MASKED_VALUE : redactValue(value)));
    return redacted;
  }

  static boolean isSensitiveKey(String key) {
    if (key == null) {
      return false;
    }
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (SAFE_KEY_SUFFIXES.stream().anyMatch(lowerKey::endsWith)) {
      return false;
    }
    String lastSegment = lowerKey.substring(lowerKey.lastIndexOf('.') + 1);
    if (SAFE_KEY_NAMES.contains(lowerKey) || SAFE_KEY_NAMES.contains(lastSegment)) {
      return false;
    }
    return SENSITIVE_KEY_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(lowerKey).matches());
  }

  /// Returns `value` with any credential in its URL userinfo replaced by the mask, keeping the scheme,
  /// host and path. Returns the value unchanged when it carries no userinfo.
  static String redactUrlUserInfo(String value) {
    if (value == null || value.indexOf('@') < 0) {
      return value;
    }
    Matcher matcher = URL_WITH_USERINFO.matcher(value);
    return matcher.matches() ? matcher.group(1) + MASKED_VALUE + "@" + matcher.group(3) : value;
  }

  /// Masks, in place, every value under a sensitive key - wholesale, whether it is a scalar, an array or
  /// a subtree - and strips URL userinfo from every remaining text value, array elements included.
  private static JsonNode redactNode(JsonNode node) {
    if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      object.fieldNames().forEachRemaining(field -> object.set(field,
          isSensitiveKey(field) ? TextNode.valueOf(MASKED_VALUE) : redactNode(object.get(field))));
    } else if (node.isArray()) {
      ArrayNode array = (ArrayNode) node;
      for (int i = 0; i < array.size(); i++) {
        array.set(i, redactNode(array.get(i)));
      }
    } else if (node.isTextual()) {
      return TextNode.valueOf(redactUrlUserInfo(node.textValue()));
    }
    return node;
  }

  private static Object redactValue(Object value) {
    if (value instanceof String) {
      return redactUrlUserInfo((String) value);
    }
    if (value instanceof List) {
      return ((List<?>) value).stream()
          .map(element -> element instanceof String ? redactUrlUserInfo((String) element) : element)
          .collect(Collectors.toList());
    }
    return value;
  }

  private static String unparseableMarker(String body, boolean truncated) {
    int capturedBytes = body.getBytes(StandardCharsets.UTF_8).length;
    if (truncated) {
      // Capture stopped at the configured cap, so the payload's real size is unknown - do not imply one.
      return String.format("[redacted: payload truncated at %d captured bytes, not parseable]",
          capturedBytes - AuditRequestProcessor.TRUNCATION_MARKER.length());
    }
    return String.format("[redacted: unparseable payload, %d bytes]", capturedBytes);
  }
}
