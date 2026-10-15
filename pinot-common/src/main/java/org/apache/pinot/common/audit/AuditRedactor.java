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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
/// The rules that follow from that are deliberate:
///
///   - **Redaction is unconditional.** There is no configuration that turns it off, because every
///     configuration that turns it off is a credential leak.
///   - **Secrets are only located inside JSON that is understood.** A complete body is parsed only if it is
///     one JSON object or array with nothing after it (see [#parseWholeValue(String)]); anything else,
///     a bare scalar included, has no key to judge it by and is reported by size only.
///   - **A body cut at the capture cap keeps its redacted prefix.** It is read token by token, so
///     everything before the cut is masked exactly as in a complete body, and only the value the cut
///     falls inside is dropped (see [#redactTruncatedPrefix(String)]).
///
/// Key matching errs towards over-redaction: masking a harmless `tokenUri` costs an audit reader
/// nothing, while missing one `accessKey` is the bug this class exists to prevent.
///
/// ## Known residual gaps
///
/// Values are inspected only for URL userinfo (`scheme://user:password@host`), which is stripped. Any
/// other credential embedded in the *value* of an innocuously named key is not caught - a JDBC URL with a
/// `password=` query parameter, a presigned URL whose signature sits inside the value, or JSON serialized
/// into a string, whose nested keys are not inspected. Callers must not read this class as a guarantee
/// that a captured payload is free of credentials.
///
/// This class is stateless and thread-safe.
final class AuditRedactor {

  static final String MASKED_VALUE = "*****";

  private static final Logger LOG = LoggerFactory.getLogger(AuditRedactor.class);

  private static final JsonFactory JSON_FACTORY = JsonUtils.DEFAULT_READER.getFactory();

  /// Separator allowed between the words of a key name, so that `accessKey`, `access_key`, `access-key`
  /// and the Hadoop-style `access.key` are treated alike.
  private static final String SEP = "[\\s._-]*";

  /// Key patterns whose values are credential material, matched against the whole lowercased key, so
  /// dotted keys such as `input.fs.prop.accessKey` are matched by their suffix. A match is final: no
  /// exemption overrides it, so a benign suffix cannot hide a credential-bearing prefix.
  ///
  /// This is a superset of [org.apache.pinot.spi.utils.Obfuscator]'s defaults, which anchor on a suffix
  /// and so miss the AWS `accessKey` / `accessKeyId` pair that motivated this class. `auth` is matched
  /// broadly rather than as `authorization`, to cover Confluent's `basic.auth.user.info` - a literal
  /// `user:password` pair present in every schema-registry-backed realtime table. A key naming a key's
  /// id, path, file or content (`sseKmsKeyId`, `gcpKeyPath`, `keyFile`, `keyContentBase64`) is sensitive.
  private static final Pattern SENSITIVE_KEY = Pattern.compile(String.join("|", ".*secret.*", ".*password.*",
      ".*passwd.*", ".*pwd", ".*passphrase.*", ".*credential.*", ".*creds.*",
      ".*key" + SEP + "(id|path|file|content).*", ".*access" + SEP + "key.*", ".*api" + SEP + "key.*",
      ".*private" + SEP + "key.*", ".*encryption" + SEP + "key.*",
      ".*signing" + SEP + "key.*", ".*keytab.*", ".*token.*", ".*auth.*", ".*jaas.*config.*", ".*signature.*",
      ".*cookie.*", ".*keystore.*", ".*truststore.*", ".*user" + SEP + "info"));

  /// Any key ending in `key` is also sensitive (`fs.s3a.access.key`, `gcpKey`, `jsonKey`,
  /// `stream.confluent.key`), except the structural names in [#SAFE_KEY_NAMES]. Kept apart from
  /// [#SENSITIVE_KEY] so that those exemptions apply to this rule alone.
  private static final Pattern ENDS_IN_KEY = Pattern.compile(".*key");

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

  /// The userinfo of every URL inside a value. Group 1 is the scheme with its `://`, which may contain
  /// colons (`jdbc:postgresql://`); group 2 runs from there to the last `@` before whitespace or the next
  /// `://`, so a password containing `@` or `/` is masked whole, and each URL of a list is handled on its
  /// own. A URL whose path contains `@` has its host masked as well - an over-redaction accepted for
  /// the sake of the password cases.
  private static final Pattern URL_USERINFO =
      Pattern.compile("([a-z][a-z0-9+.:-]*://)((?:(?!://)\\S)*)@", Pattern.CASE_INSENSITIVE);

  private AuditRedactor() {
  }

  /// Returns the captured request body with every credential-bearing value masked. A complete body that is
  /// not one parseable JSON object or array is reported by size only. A truncated body keeps its redacted
  /// JSON prefix, followed by [AuditRequestProcessor#TRUNCATION_MARKER], or is reported by size if no JSON
  /// object or array begins it.
  ///
  /// @param body the captured body, possibly null
  /// @return the redacted body, or null if there was nothing to record
  static String redactBody(AuditRequestProcessor.CapturedBody body) {
    if (body == null || StringUtils.isBlank(body.getText())) {
      return null;
    }
    try {
      if (body.isTruncated()) {
        String prefix = redactTruncatedPrefix(body.getText());
        if (prefix != null) {
          return prefix + AuditRequestProcessor.TRUNCATION_MARKER;
        }
      } else {
        JsonNode parsed = parseWholeValue(body.getText());
        if (parsed != null && parsed.isContainerNode()) {
          return JsonUtils.objectToString(redactNode(parsed));
        }
      }
    } catch (Exception e) {
      // Never log the exception itself. Jackson embeds the offending token in its message, so a body
      // that is a bare credential would write that credential into the log this class protects.
      LOG.debug("Audit payload is not parseable JSON ({}); recording size only", e.getClass().getSimpleName());
    }
    return body.isTruncated()
        ? String.format("[redacted: payload truncated at %d captured bytes, not parseable]", body.getCapturedBytes())
        : String.format("[redacted: unparseable payload, %d bytes]", body.getCapturedBytes());
  }

  /// Parses `body` only if it is a single complete JSON value with nothing following it.
  ///
  /// [com.fasterxml.jackson.databind.ObjectReader#readTree(String)] stops at the first value and
  /// silently discards whatever follows, because `FAIL_ON_TRAILING_TOKENS` is off by default. Relying on
  /// it would record a concatenated payload as a well-formed body with the remainder quietly dropped.
  /// Reading through an explicit parser and then demanding the stream be exhausted rejects that, so it
  /// falls to the size-only path instead.
  ///
  /// @return the parsed value, or null if the body is empty or carries trailing content
  private static JsonNode parseWholeValue(String body)
      throws IOException {
    try (JsonParser parser = JSON_FACTORY.createParser(body)) {
      JsonNode node = JsonUtils.DEFAULT_READER.readTree(parser);
      if (node == null || node.isMissingNode() || parser.nextToken() != null) {
        return null;
      }
      return node;
    }
  }

  /// Returns the redacted JSON that precedes the cut in a truncated body, with the objects and arrays the
  /// cut left open closed again, or null if the body does not begin with a JSON object or array.
  ///
  /// The prefix is copied token by token rather than parsed as a tree, which a truncated body can never
  /// be. A sensitive key is masked as soon as its name is complete, whether or not its value made it in
  /// before the cut. Any other name is written only together with its value, and the value the cut falls
  /// inside is dropped rather than recorded as if it were complete: a string or literal missing its end
  /// fails to parse, and a number that runs up to the cut may be missing digits, so it is dropped too.
  private static String redactTruncatedPrefix(String text)
      throws IOException {
    StringWriter out = new StringWriter();
    // The generator is closed first, and closing it writes the end of every object and array still open.
    try (JsonParser parser = JSON_FACTORY.createParser(text);
        JsonGenerator generator = JSON_FACTORY.createGenerator(out)) {
      JsonToken first = parser.nextToken();
      if (first != JsonToken.START_OBJECT && first != JsonToken.START_ARRAY) {
        return null;
      }
      try {
        copyRedacted(parser, generator, first, text.length());
      } catch (IOException e) {
        // Expected: the cut leaves the last token incomplete, and everything before it is already written.
      }
    }
    return out.toString();
  }

  private static void copyRedacted(JsonParser parser, JsonGenerator generator, JsonToken first, int textLength)
      throws IOException {
    String pendingName = null;
    JsonToken token = first;
    while (token != null) {
      switch (token) {
        case FIELD_NAME:
          String name = parser.currentName();
          if (isSensitiveKey(name)) {
            generator.writeStringField(name, MASKED_VALUE);
            JsonToken value = parser.nextToken();
            if (value != null && value.isStructStart()) {
              parser.skipChildren();
            }
          } else {
            pendingName = name;
          }
          token = parser.nextToken();
          break;
        case START_OBJECT:
        case START_ARRAY:
          if (pendingName != null) {
            generator.writeFieldName(pendingName);
            pendingName = null;
          }
          generator.copyCurrentEvent(parser);
          token = parser.nextToken();
          break;
        case END_OBJECT:
        case END_ARRAY:
          generator.copyCurrentEvent(parser);
          token = parser.nextToken();
          break;
        default:
          if (token.isNumeric() && parser.currentLocation().getCharOffset() >= textLength) {
            return;
          }
          String text = token == JsonToken.VALUE_STRING ? redactUrlUserInfo(parser.getText()) : parser.getText();
          if (pendingName != null) {
            generator.writeFieldName(pendingName);
            pendingName = null;
          }
          writeScalar(generator, token, text);
          token = parser.nextToken();
          break;
      }
    }
  }

  private static void writeScalar(JsonGenerator generator, JsonToken scalar, String text)
      throws IOException {
    switch (scalar) {
      case VALUE_NUMBER_INT:
      case VALUE_NUMBER_FLOAT:
        generator.writeNumber(text);
        break;
      case VALUE_TRUE:
      case VALUE_FALSE:
        generator.writeBoolean(scalar == JsonToken.VALUE_TRUE);
        break;
      case VALUE_NULL:
        generator.writeNull();
        break;
      default:
        generator.writeString(text);
        break;
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
    if (SENSITIVE_KEY.matcher(lowerKey).matches()) {
      return true;
    }
    String lastSegment = lowerKey.substring(lowerKey.lastIndexOf('.') + 1);
    return ENDS_IN_KEY.matcher(lowerKey).matches() && !SAFE_KEY_NAMES.contains(lowerKey)
        && !SAFE_KEY_NAMES.contains(lastSegment);
  }

  /// Returns `value` with the userinfo of every URL in it replaced by the mask, keeping each scheme, host
  /// and path. Returns the value unchanged when it carries no userinfo.
  static String redactUrlUserInfo(String value) {
    if (value == null || value.indexOf('@') < 0) {
      return value;
    }
    return URL_USERINFO.matcher(value)
        .replaceAll(match -> Matcher.quoteReplacement(match.group(1) + MASKED_VALUE + "@"));
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
}
