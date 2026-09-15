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
package org.apache.pinot.segment.spi.codec;

import java.util.List;
import java.util.Objects;


/// Stateless, thread-safe, package-private source of truth for the codec DSL's structural limits and token validation.
final class CodecDslSyntax {
  static final int MAX_SPEC_LENGTH = 4096;
  static final int MAX_PIPELINE_STAGES = 32;
  static final int MAX_IDENTIFIER_LENGTH = 128;
  static final int MAX_ARGS_PER_INVOCATION = 16;
  static final int MAX_ARGUMENT_LENGTH = 32;
  static final String REMOVED_WRAPPER_NAME = "CODEC";

  private CodecDslSyntax() {
  }

  static void validateName(String name) {
    if (name.isEmpty() || name.length() > MAX_IDENTIFIER_LENGTH || !isAsciiIdentifierStart(name.charAt(0))) {
      throw new IllegalArgumentException("Invalid codec name: " + name);
    }
    for (int i = 1; i < name.length(); i++) {
      if (!isAsciiIdentifierPart(name.charAt(i))) {
        throw new IllegalArgumentException("Invalid codec name: " + name);
      }
    }
  }

  static void validateArguments(List<String> args) {
    if (args.size() > MAX_ARGS_PER_INVOCATION) {
      throw new IllegalArgumentException(
          "Too many codec arguments: " + args.size() + " (max " + MAX_ARGS_PER_INVOCATION + ")");
    }
    for (String arg : args) {
      validateArgument(Objects.requireNonNull(arg, "arg"));
    }
  }

  static void validateArgument(String arg) {
    if (arg.isEmpty() || arg.length() > MAX_ARGUMENT_LENGTH) {
      throw new IllegalArgumentException("Invalid codec argument: " + arg);
    }
    for (int i = 0; i < arg.length(); i++) {
      if (!isAsciiDigit(arg.charAt(i))) {
        throw new IllegalArgumentException("Invalid codec argument: " + arg);
      }
    }
    if (arg.length() > 1 && arg.charAt(0) == '0') {
      throw new IllegalArgumentException("Leading zeros are not allowed in codec argument: " + arg);
    }
  }

  static boolean isRemovedWrapperName(String name) {
    return REMOVED_WRAPPER_NAME.equalsIgnoreCase(name);
  }

  static boolean isAsciiIdentifierStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
  }

  static boolean isAsciiIdentifierPart(char c) {
    return isAsciiIdentifierStart(c) || isAsciiDigit(c);
  }

  static boolean isAsciiDigit(char c) {
    return c >= '0' && c <= '9';
  }
}
