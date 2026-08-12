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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;


/// Simple obfuscator for object trees and configuration containers with key-value pairs. Matches a configurable set of
/// patterns and replaces sensitive values with a pre-defined masked value for output.
///
/// Example input:
///
/// ```
/// {
///   "type": "sample object",
///   "connection": {
///     "user": "admin",
///     "password": "verysecret"
///   }
/// }
/// ```
///
/// Example output
///
/// ```
/// {
///   "type": "sample object",
///   "connection": {
///     "user": "admin",
///     "password": "*****"
///   }
/// }
/// ```
public final class Obfuscator {

  private static final String DEFAULT_MASKED_VALUE = "*****";
  /// Suffix-anchored, so that a key is masked for what it ends in and a structural name such as
  /// `pinot.broker.auth.provider.class` stays readable. Beyond the generic credential words, this covers
  /// credentials whose key does not end in one: the AWS access and secret key pair, dotted or not (as in
  /// `input.fs.prop.accessKey`, `fs.s3a.access.key` and `fs.s3a.secret.key`), the Azure storage
  /// `account.key`, Kafka's `sasl.jaas.config`, Confluent's `basic.auth.user.info` (a literal
  /// `user:password`), and inline private or service-account key material. Key *file* names are not
  /// masked, because their values are paths rather than secrets.
  private static final List<Pattern> DEFAULT_PATTERNS =
      Stream.of("(?i).*secret$", "(?i).*secret[\\s._-]*key$", "(?i).*api[\\s._-]*key$", "(?i).*password$",
              "(?i).*passwd$", "(?i).*passphrase$", "(?i).*keytab$", "(?i).*token$", "(?i).*credentials?$",
              "(?i).*access[\\s._-]*key([\\s._-]*id)?$", "(?i).*private[\\s._-]*key([\\s._-]*id)?$",
              "(?i).*account[\\s._-]*key$", "(?i).*jaas[\\s._-]*config$", "(?i).*user[\\s._-]*info$",
              "(?i).*json[\\s._-]*key$", "(?i).*key[\\s._-]*content([\\s._-]*base64)?$")
          .map(Pattern::compile).collect(Collectors.toList());
  public static final Obfuscator DEFAULT = new Obfuscator();

  private final String _maskedValue;
  private final List<Pattern> _patterns;
  private final boolean _useDefaultCredentialPolicy;

  /// Obfuscator using the default credential suffix patterns (see [#DEFAULT_PATTERNS]) and the shared credential
  /// policy. It masks sensitive property names and credential-shaped content in textual values, including assignments
  /// and URI user-info or query parameters, while preserving whole unresolved placeholders. Masked values use
  /// '\*\*\*\*\*'.
  public Obfuscator() {
    _maskedValue = DEFAULT_MASKED_VALUE;
    _patterns = DEFAULT_PATTERNS;
    _useDefaultCredentialPolicy = true;
  }

  /// Obfuscator with customized masking behavior. Defaults do not apply! Please ensure case-insensitive regex matching.
  ///
  /// @param maskedValue replacement value
  /// @param patterns key patterns to obfuscate
  public Obfuscator(String maskedValue, List<Pattern> patterns) {
    _maskedValue = maskedValue;
    _patterns = patterns;
    _useDefaultCredentialPolicy = false;
  }

  /// Serialize an object tree to JSON and obfuscate matching keys. This method handles special cases for JsonNode and
  /// String objects separately to minimize surprises.
  ///
  /// @param object input tree
  /// @return obfuscated JSON tree
  public JsonNode toJson(Object object) {
    // NOTE: jayway json path 2.4.0 seems to have issues with '@.name' so we'll do this manually
    // as determined by a cursory and purely subjective investigation by alex
    // "$..[?(@.name =~ /password$/i || @.name =~ /secret$/i || @.name =~ /secret[\\s_-]*key$/i || @.name =~ /keytab$/i
    //     || @.name =~ /token$/i)]"

    try {
      JsonNode node;
      if (object instanceof JsonNode) {
        node = (JsonNode) object;
      } else if (object instanceof String) {
        node = JsonUtils.stringToJsonNode(String.valueOf(object));
      } else {
        node = JsonUtils.objectToJsonNode(object);
      }
      return toJsonRecursive(node);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Serialize an object tree to a JSON string and obfuscate matching keys.
  ///
  /// @param object input tree
  /// @return obfuscated JSON string
  public String toJsonString(Object object) {
    return String.valueOf(toJson(object));
  }

  private JsonNode toJsonRecursive(JsonNode node) {
    if (node.isObject()) {
      node.fieldNames().forEachRemaining(field -> {
        JsonNode fieldValue = node.get(field);
        boolean preservePlaceholder = _useDefaultCredentialPolicy && fieldValue.isTextual()
            && TableConfigRedactionUtils.isUnresolvedPlaceholder(fieldValue.textValue());
        boolean sensitive = _patterns.stream().anyMatch(pattern -> pattern.matcher(field).matches())
            || (_useDefaultCredentialPolicy && TableConfigRedactionUtils.isSensitivePropertyName(field));
        if (sensitive && !preservePlaceholder) {
          ((ObjectNode) node).put(field, _maskedValue);
        } else {
          ((ObjectNode) node).set(field, toJsonRecursive(fieldValue));
        }
      });
    } else if (node.isArray()) {
      IntStream.range(0, node.size()).forEach(i -> ((ArrayNode) node).set(i, toJsonRecursive(node.get(i))));
    } else if (_useDefaultCredentialPolicy && node.isTextual()) {
      return TextNode.valueOf(TableConfigRedactionUtils.redactStructuredText(node.textValue()));
    }

    return node;
  }
}
