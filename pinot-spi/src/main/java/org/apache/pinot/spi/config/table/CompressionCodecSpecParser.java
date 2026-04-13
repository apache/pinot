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

import java.util.regex.Pattern;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * Parser for user-facing compression codec syntax.
 *
 * <p>This class is thread-safe because it is stateless.
 */
public final class CompressionCodecSpecParser {
  private static final Pattern CODEC_NAME_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private CompressionCodecSpecParser() {
  }

  public static CompressionCodecSpec parse(String rawSpec) {
    if (rawSpec == null) {
      throw new IllegalArgumentException("Compression codec specification must not be null.");
    }
    String trimmedSpec = rawSpec.trim();
    if (trimmedSpec.isEmpty()) {
      throw new IllegalArgumentException("Compression codec specification must not be empty.");
    }

    int openParenIndex = trimmedSpec.indexOf('(');
    if (openParenIndex < 0) {
      CompressionCodec codec = parseCodecName(trimmedSpec, rawSpec);
      return CompressionCodecSpec.of(codec, null);
    }

    int closeParenIndex = trimmedSpec.indexOf(')', openParenIndex + 1);
    if (closeParenIndex < 0 || closeParenIndex != trimmedSpec.length() - 1
        || trimmedSpec.indexOf('(', openParenIndex + 1) >= 0 || trimmedSpec.indexOf(')', closeParenIndex + 1) >= 0) {
      throwInvalidSpec(rawSpec);
    }

    String codecToken = trimmedSpec.substring(0, openParenIndex).trim();
    String levelToken = trimmedSpec.substring(openParenIndex + 1, closeParenIndex).trim();
    if (codecToken.isEmpty()) {
      throwInvalidSpec(rawSpec);
    }
    if (levelToken.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid compression level in specification '%s'. Expected a non-empty integer.", rawSpec));
    }
    if (!levelToken.chars().allMatch(Character::isDigit)) {
      throw new IllegalArgumentException(
          String.format("Invalid compression level '%s' in specification '%s'. Expected an integer.", levelToken,
              rawSpec));
    }

    CompressionCodec codec = parseCodecName(codecToken, rawSpec);
    Integer level = Integer.parseInt(levelToken);
    CompressionCodecCapabilities.validateLevelSupport(codec, level, rawSpec);
    return CompressionCodecSpec.of(codec, level);
  }

  private static CompressionCodec parseCodecName(String codecToken, String rawSpec) {
    if (!CODEC_NAME_PATTERN.matcher(codecToken).matches()) {
      throwInvalidSpec(rawSpec);
    }
    try {
      return CompressionCodecCapabilities.canonicalizeAlias(codecToken);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unknown compression codec '%s' in specification '%s'.", codecToken, rawSpec), e);
    }
  }

  private static void throwInvalidSpec(String rawSpec) {
    throw new IllegalArgumentException(
        String.format("Invalid compression codec specification '%s'. Expected CODEC or CODEC(level).", rawSpec));
  }
}
