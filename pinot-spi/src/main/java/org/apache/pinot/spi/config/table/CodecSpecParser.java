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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * Parses a codec specification string into a {@link CodecSpec}.
 *
 * <p>Accepted formats:
 * <ul>
 *   <li>{@code NAME} — codec with no parameters (e.g. {@code "SNAPPY"})</li>
 *   <li>{@code NAME(level)} — codec with a single integer level (e.g. {@code "ZSTD(3)"})</li>
 * </ul>
 *
 * <p>Parsing is case-insensitive; the resulting name is always uppercase.
 * Known aliases (e.g. {@code ZSTD} to {@code ZSTANDARD}) are normalized automatically.
 *
 * <p>This class is a static utility and cannot be instantiated.
 */
public final class CodecSpecParser {

  /**
   * Alias map from shorthand names to canonical names.
   */
  private static final Map<String, String> ALIASES;

  static {
    Map<String, String> aliases = new HashMap<>();
    aliases.put("ZSTD", "ZSTANDARD");
    ALIASES = Collections.unmodifiableMap(aliases);
  }

  private CodecSpecParser() {
  }

  /**
   * Parses a raw codec specification string into a {@link CodecSpec}.
   *
   * @param raw the raw string, e.g. {@code "ZSTD(3)"}, {@code "SNAPPY"}, {@code "lz4hc(5)"}
   * @return a parsed {@link CodecSpec} with a canonicalized uppercase name
   * @throws IllegalArgumentException if the input is null, empty, or malformed
   */
  public static CodecSpec parse(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalArgumentException("Codec specification must not be null or empty");
    }
    String trimmed = raw.trim();

    int openParen = trimmed.indexOf('(');
    int closeParen = trimmed.indexOf(')');

    if (openParen < 0 && closeParen < 0) {
      // Simple name, no parentheses
      String name = canonicalize(trimmed);
      return CodecSpec.of(name);
    }

    // Validate balanced parentheses
    if (openParen < 0 || closeParen < 0) {
      throw new IllegalArgumentException("Unbalanced parentheses in codec specification: " + raw);
    }
    if (closeParen != trimmed.length() - 1) {
      throw new IllegalArgumentException("Unexpected characters after closing parenthesis in: " + raw);
    }
    if (openParen == 0) {
      throw new IllegalArgumentException("Missing codec name before '(' in: " + raw);
    }

    String namePart = trimmed.substring(0, openParen);
    String argPart = trimmed.substring(openParen + 1, closeParen);

    // Reject empty parens
    if (argPart.isEmpty()) {
      throw new IllegalArgumentException("Empty parentheses in codec specification: " + raw);
    }

    // Reject multiple arguments
    if (argPart.contains(",")) {
      throw new IllegalArgumentException("Multiple arguments are not supported in codec specification: " + raw);
    }

    // Parse the single positional argument as an integer level
    int level;
    try {
      level = Integer.parseInt(argPart.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Expected integer level argument but got '" + argPart.trim() + "' in: " + raw);
    }

    String name = canonicalize(namePart);
    Map<String, String> params = Collections.singletonMap("level", String.valueOf(level));
    return CodecSpec.of(name, params);
  }

  /**
   * Canonicalizes a codec name: uppercases and resolves aliases.
   */
  private static String canonicalize(String name) {
    String upper = name.toUpperCase(Locale.ENGLISH);
    return ALIASES.getOrDefault(upper, upper);
  }
}
