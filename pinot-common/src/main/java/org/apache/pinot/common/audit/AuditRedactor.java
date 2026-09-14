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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.Obfuscator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Masks credential-bearing values out of captured audit payloads before they are serialized into an
/// [AuditEvent].
///
/// Audit records are written to stdout and to a rotating file, and are usually shipped onward to a log
/// pipeline whose operators are not entitled to the credentials a caller happens to send. Anything that
/// leaves this class must therefore be assumed to be readable by more people than the request itself was.
///
/// Two rules follow from that, and both are deliberate:
///
///   - **Redaction is unconditional.** There is no configuration that turns it off, because every
///     configuration that turns it off is a credential leak.
///   - **A body that cannot be parsed is reported by size only.** Secrets can only be located inside a
///     structure that is understood; in an opaque payload they cannot be found, so nothing of it is kept.
///     This includes bodies truncated by [AuditConfig#getMaxPayloadSize()], whose trailing fragment is no
///     longer valid JSON.
///
/// Key matching errs towards over-redaction: masking a harmless `tokenUri` costs an audit reader nothing,
/// while missing one `accessKey` is the bug this class exists to prevent.
///
/// This class is stateless and thread-safe.
final class AuditRedactor {

  static final String MASKED_VALUE = "*****";

  private static final Logger LOG = LoggerFactory.getLogger(AuditRedactor.class);

  /// Key patterns whose values are credential material. Applied case-insensitively against the full key,
  /// so dotted keys such as `input.fs.prop.accessKey` are matched by their suffix.
  ///
  /// This is a superset of [Obfuscator]'s defaults, which anchor on a suffix and so miss the AWS
  /// `accessKey` / `accessKeyId` pair that motivated this class.
  private static final List<Pattern> SENSITIVE_KEY_PATTERNS =
      Stream.of(".*secret.*", ".*password.*", ".*passwd.*", ".*pwd$", ".*passphrase.*", ".*credential.*",
              ".*access[\\s_-]*key.*", ".*api[\\s_-]*key.*", ".*private[\\s_-]*key.*", ".*encryption[\\s_-]*key.*",
              ".*signing[\\s_-]*key.*", ".*keytab.*", ".*token.*", ".*authorization.*", ".*jaas.*config.*",
              ".*signature.*")
          .map(pattern -> Pattern.compile("(?i)" + pattern))
          .collect(Collectors.toList());

  private static final Obfuscator OBFUSCATOR = new Obfuscator(MASKED_VALUE, SENSITIVE_KEY_PATTERNS);

  private AuditRedactor() {
  }

  /// Returns the request body with every credential-bearing value masked, or a size-only placeholder when
  /// the body is not parseable JSON.
  ///
  /// @param body the raw captured body, possibly null or blank
  /// @return the redacted body, or null if there was nothing to record
  static String redactBody(String body) {
    if (body == null || body.isEmpty()) {
      return null;
    }
    try {
      JsonNode redacted = OBFUSCATOR.toJson(JsonUtils.stringToJsonNode(body));
      return JsonUtils.objectToString(redacted);
    } catch (Exception e) {
      // Not JSON, or truncated mid-structure. Credentials cannot be located in it, so keep only its size.
      LOG.debug("Audit payload is not parseable JSON; recording size only", e);
      return unparseableMarker(body);
    }
  }

  /// Returns a copy of {@code map} with the values of credential-bearing keys masked. Multi-valued entries
  /// are replaced wholesale rather than element-wise, so the number of values is not disclosed either.
  static Map<String, Object> redact(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return map;
    }
    Map<String, Object> redacted = new HashMap<>(map.size());
    map.forEach((key, value) -> redacted.put(key, isSensitiveKey(key) ? MASKED_VALUE : value));
    return redacted;
  }

  static boolean isSensitiveKey(String key) {
    return key != null && SENSITIVE_KEY_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(key).matches());
  }

  private static String unparseableMarker(String body) {
    return String.format("[redacted: unparseable payload, %d bytes]",
        body.getBytes(StandardCharsets.UTF_8).length);
  }
}
