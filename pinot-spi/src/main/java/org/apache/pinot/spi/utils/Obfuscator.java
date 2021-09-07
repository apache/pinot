package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Simple obfuscator for object trees and configuration containers with key-value pairs. Matches a configurable set of
 * patterns and replaces sensitive values with a pre-defined masked value for output.
 *
 * Example input:
 * <pre>
 *   {
 *     "type": "sample object",
 *     "nestedCredentials": {
 *       "user": "admin",
 *       "password": "verysecret"
 *     }
 *   }
 * </pre>
 *
 * Example output
 * <pre>
 *   {
 *     "type": "sample object",
 *     "nestedCredentials": {
 *       "user": "admin",
 *       "password": "*****"
 *     }
 *   }
 * </pre>
 */
public final class Obfuscator {
  private static final String DEFAULT_MASKED_VALUE = "*****";
  private static final List<Pattern> DEFAULT_PATTERNS =
      Stream.of("(?i).*secret$", "(?i).*password$", "(?i).*token$").map(Pattern::compile).collect(Collectors.toList());

  private final String _maskedValue;
  private final List<Pattern> _patterns;

  /**
   * Obfuscator with default behavior matching (ignore case) "secret", "password", and "token" suffixes. Masks any
   * values with '*****'
   */
  public Obfuscator() {
    _maskedValue = DEFAULT_MASKED_VALUE;
    _patterns = DEFAULT_PATTERNS;
  }

  /**
   * Obfuscator with customized masking behavior. Defaults do not apply! Please ensure case-insensitive regex matching.
   *
   * @param maskedValue replacement value
   * @param patterns key patterns to obfuscate
   */
  public Obfuscator(String maskedValue, List<Pattern> patterns) {
    _maskedValue = maskedValue;
    _patterns = patterns;
  }

  /**
   * Serialize an object tree as JSON and obfuscate matching keys.
   *
   * @param object input value
   * @return obfuscated JSON tree
   */
  public JsonNode obfuscateJson(Object object) {
    // NOTE: jayway json path 2.4.0 seems to have issues with '@.name' so we'll do this manually
    // as determined by a cursory and purely subjective investigation by alex
    // "$..[?(@.name =~ /password$/i || @.name =~ /secret$/i || @.name =~ /token$/i)]"

    return obfuscateJsonRec(JsonUtils.objectToJsonNode(object));
  }

  private JsonNode obfuscateJsonRec(JsonNode node) {
    if (node.isObject()) {
      node.fieldNames().forEachRemaining(field -> {
        if (_patterns.stream().anyMatch(pattern -> pattern.matcher(field).matches())) {
          ((ObjectNode) node).put(field, _maskedValue);
        } else if (node.isArray()) {
          IntStream.range(0, node.size()).forEach(i -> ((ArrayNode) node).set(i, obfuscateJsonRec(node.get(i))));
        } else if (node.isObject()) {
          ((ObjectNode) node).put(field, obfuscateJsonRec(node.get(field)));
        }
      });
    }

    return node;
  }
}
