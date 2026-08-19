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


/// Immutable and thread-safe AST node for one codec invocation, such as `DELTA` or `ZSTD(3)`.
///
/// Argument values remain strings after structural parsing. Codec implementations are responsible
/// for interpreting and semantically validating them.
public final class CodecInvocation {
  private final String _name;
  private final List<String> _args;

  /// @param name codec name using the DSL's ASCII identifier syntax
  /// @param args positional numeric arguments
  public CodecInvocation(String name, List<String> args) {
    String checkedName = Objects.requireNonNull(name, "name");
    validateName(checkedName);
    _name = checkedName.toUpperCase(Locale.ROOT);

    List<String> checkedArgs = Objects.requireNonNull(args, "args");
    if (checkedArgs.size() > CodecSpecParser.MAX_ARGS_PER_INVOCATION) {
      throw new IllegalArgumentException(
          "Too many codec arguments: " + checkedArgs.size() + " (max "
              + CodecSpecParser.MAX_ARGS_PER_INVOCATION + ")");
    }
    _args = List.copyOf(checkedArgs);
    validateArgs(_args);
  }

  /// Returns the codec name normalized to upper case.
  public String name() {
    return _name;
  }

  /// Returns the immutable positional arguments.
  public List<String> args() {
    return _args;
  }

  @Override
  public String toString() {
    return _args.isEmpty() ? _name : _name + "(" + String.join(",", _args) + ")";
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
    if (CodecSpecParser.REMOVED_WRAPPER_NAME.equalsIgnoreCase(name)) {
      throw new IllegalArgumentException("CODEC is reserved by the removed wrapper syntax");
    }
  }

  private static void validateArgs(List<String> args) {
    for (String arg : args) {
      if (arg.isEmpty() || arg.length() > CodecSpecParser.MAX_ARGUMENT_LENGTH) {
        throw new IllegalArgumentException("Invalid codec argument: " + arg);
      }
      for (int i = 0; i < arg.length(); i++) {
        char c = arg.charAt(i);
        if (c < '0' || c > '9') {
          throw new IllegalArgumentException("Invalid codec argument: " + arg);
        }
      }
      // Reject leading zeros so each argument value has exactly one spelling in the canonical form that is
      // later frozen into segment headers.
      if (arg.length() > 1 && arg.charAt(0) == '0') {
        throw new IllegalArgumentException("Invalid codec argument (leading zeros): " + arg);
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
