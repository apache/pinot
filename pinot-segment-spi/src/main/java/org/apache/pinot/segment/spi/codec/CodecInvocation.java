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
import java.util.Locale;
import java.util.Objects;


/// AST node representing a single codec invocation in the DSL, e.g. `ZSTD(3)` or
/// `DELTA`.
///
/// Produced by [CodecSpecParser] during Phase 1 (structural) parsing.  The raw
/// [#args()] are still strings at this point; codec-specific semantic parsing happens
/// in Phase 2 via [CodecDefinition#parseOptions()].
///
/// Instances are immutable and thread-safe.
public final class CodecInvocation {
  private final String _name;
  private final List<String> _args;

  /// @param name the codec name; ASCII identifier syntax is required and lower-case letters are normalized to
  ///             upper-case
  /// @param args positional arguments; copied defensively and must not be `null` (use an empty list for no args)
  /// @throws NullPointerException if `name`, `args`, or an argument is `null`
  /// @throws IllegalArgumentException if the name or arguments do not follow the structural DSL limits
  public CodecInvocation(String name, List<String> args) {
    String checkedName = Objects.requireNonNull(name, "name");
    validateName(checkedName);
    _name = checkedName.toUpperCase(Locale.ROOT);

    List<String> checkedArgs = Objects.requireNonNull(args, "args");
    if (checkedArgs.size() > CodecSpecParser.MAX_ARGS_PER_STAGE) {
      throw new IllegalArgumentException(
          "Too many codec arguments: " + checkedArgs.size() + " (max " + CodecSpecParser.MAX_ARGS_PER_STAGE + ")");
    }
    _args = List.copyOf(checkedArgs);
    validateArgs(_args);
  }

  /// Returns the codec name, normalized to upper-case.
  public String name() {
    return _name;
  }

  /// Returns the immutable raw positional arguments; never `null`, may be empty.
  public List<String> args() {
    return _args;
  }

  @Override
  public String toString() {
    if (_args.isEmpty()) {
      return _name;
    }
    return _name + "(" + String.join(",", _args) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CodecInvocation)) {
      return false;
    }
    CodecInvocation that = (CodecInvocation) o;
    return _name.equals(that._name) && _args.equals(that._args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _args);
  }

  private static void validateName(String name) {
    if (name.isEmpty() || name.length() > CodecSpecParser.MAX_IDENTIFIER_LENGTH
        || !isAsciiIdentifierStart(name.charAt(0))) {
      throw new IllegalArgumentException("Invalid codec name: " + name);
    }
    for (int i = 1; i < name.length(); i++) {
      if (!isAsciiIdentifierPart(name.charAt(i))) {
        throw new IllegalArgumentException("Invalid codec name: " + name);
      }
    }
  }

  private static void validateArgs(List<String> args) {
    for (String arg : args) {
      if (arg.isEmpty() || arg.length() > CodecSpecParser.MAX_ARG_LENGTH) {
        throw new IllegalArgumentException("Invalid codec argument: " + arg);
      }
      for (int i = 0; i < arg.length(); i++) {
        char c = arg.charAt(i);
        if (c < '0' || c > '9') {
          throw new IllegalArgumentException("Invalid codec argument: " + arg);
        }
      }
    }
  }

  private static boolean isAsciiIdentifierStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
  }

  private static boolean isAsciiIdentifierPart(char c) {
    return isAsciiIdentifierStart(c) || (c >= '0' && c <= '9');
  }
}
