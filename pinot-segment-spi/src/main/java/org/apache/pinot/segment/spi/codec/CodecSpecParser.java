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

import java.util.ArrayList;
import java.util.List;


/// Stateless and thread-safe structural parser for the codec DSL.
///
/// The grammar is:
/// ```
/// spec       ::= invocation ("," invocation)*
/// invocation ::= NAME | NAME "(" args ")"
/// args       ::= ε | arg ("," arg)*
/// arg        ::= "0" | [1-9][0-9]*
/// NAME       ::= [A-Za-z_][A-Za-z0-9_]*
/// ```
///
/// Examples are `DELTA`, `ZSTD(3)`, and `DELTA,ZSTD(3)`. The removed
/// `CODEC(...)` wrapper is deliberately not accepted. This parser validates only structure;
/// codec names, arguments, and type compatibility are validated by the codec runtime.
public final class CodecSpecParser {
  public static final int MAX_SPEC_LENGTH = 4096;
  public static final int MAX_PIPELINE_STAGES = 32;
  public static final int MAX_IDENTIFIER_LENGTH = 128;
  public static final int MAX_ARGS_PER_INVOCATION = 16;
  public static final int MAX_ARGUMENT_LENGTH = 32;

  /// Reserved codec name from the removed `CODEC(...)` wrapper syntax. Public so every layer that
  /// enforces the reservation (this parser, [CodecInvocation], and the codec registry) shares one
  /// definition instead of re-spelling the literal.
  public static final String REMOVED_WRAPPER_NAME = "CODEC";

  private CodecSpecParser() {
  }

  /// Parses the codec specification into an immutable AST.
  ///
  /// @param spec codec DSL string
  /// @return parsed codec pipeline
  /// @throws IllegalArgumentException if the specification is null, blank, too large, or structurally invalid
  public static CodecPipeline parse(String spec) {
    if (spec == null) {
      throw new IllegalArgumentException("codecSpec must not be null or blank");
    }
    if (spec.length() > MAX_SPEC_LENGTH) {
      throw new IllegalArgumentException(
          "codecSpec length " + spec.length() + " exceeds maximum " + MAX_SPEC_LENGTH);
    }
    if (spec.isBlank()) {
      throw new IllegalArgumentException("codecSpec must not be null or blank");
    }
    String trimmed = spec.trim();

    Parser parser = new Parser(trimmed);
    List<CodecInvocation> stages = new ArrayList<>();
    parser.addStage(stages, parser.parseInvocation());
    while (parser.peek() == ',') {
      parser.consume(',');
      parser.addStage(stages, parser.parseInvocation());
    }
    parser.expectEof();
    return new CodecPipeline(stages);
  }

  private static final class Parser {
    private final String _input;
    private int _pos;

    private Parser(String input) {
      _input = input;
    }

    private void addStage(List<CodecInvocation> stages, CodecInvocation invocation) {
      if (stages.size() >= MAX_PIPELINE_STAGES) {
        throw new IllegalArgumentException(
            "Codec pipeline exceeds maximum " + MAX_PIPELINE_STAGES + " stages in: " + _input);
      }
      stages.add(invocation);
    }

    private CodecInvocation parseInvocation() {
      String name = parseName();
      if (REMOVED_WRAPPER_NAME.equalsIgnoreCase(name)) {
        throw new IllegalArgumentException(
            "CODEC(...) wrapper is not supported; list codec invocations directly in: " + _input);
      }
      List<String> args = List.of();
      if (peek() == '(') {
        consume('(');
        args = parseArgs();
        consume(')');
      }
      return new CodecInvocation(name, args);
    }

    private String parseName() {
      skipWhitespace();
      if (_pos >= _input.length()) {
        throw new IllegalArgumentException("Expected codec name but reached end of input: " + _input);
      }
      char first = _input.charAt(_pos);
      if (!isAsciiIdentifierStart(first)) {
        throw new IllegalArgumentException(
            "Expected codec name starting with [A-Za-z_] at position " + _pos + " in: " + _input);
      }
      int start = _pos++;
      while (_pos < _input.length() && isAsciiIdentifierPart(_input.charAt(_pos))) {
        _pos++;
      }
      if (_pos - start > MAX_IDENTIFIER_LENGTH) {
        throw new IllegalArgumentException(
            "Codec name length " + (_pos - start) + " exceeds maximum " + MAX_IDENTIFIER_LENGTH + " in: "
                + _input);
      }
      return _input.substring(start, _pos);
    }

    private List<String> parseArgs() {
      skipWhitespace();
      if (peek() == ')') {
        return List.of();
      }
      List<String> args = new ArrayList<>();
      args.add(parseArg());
      while (peek() == ',') {
        if (args.size() >= MAX_ARGS_PER_INVOCATION) {
          throw new IllegalArgumentException(
              "Codec invocation exceeds maximum " + MAX_ARGS_PER_INVOCATION + " arguments in: " + _input);
        }
        consume(',');
        args.add(parseArg());
      }
      return args;
    }

    private String parseArg() {
      skipWhitespace();
      if (_pos >= _input.length()) {
        throw new IllegalArgumentException("Expected argument but reached end of input: " + _input);
      }
      int start = _pos;
      char first = _input.charAt(_pos);
      if (first == '-' || first == '+') {
        throw new IllegalArgumentException(
            "Leading sign is not allowed in codec argument at position " + start + " in: " + _input);
      }
      if (first < '0' || first > '9') {
        throw new IllegalArgumentException("Expected integer argument at position " + start + " in: " + _input);
      }
      while (_pos < _input.length()) {
        char c = _input.charAt(_pos);
        if (c < '0' || c > '9') {
          break;
        }
        _pos++;
      }
      if (_pos - start > MAX_ARGUMENT_LENGTH) {
        throw new IllegalArgumentException(
            "Codec argument length " + (_pos - start) + " exceeds maximum " + MAX_ARGUMENT_LENGTH + " in: "
                + _input);
      }
      // Leading zeros are rejected so that every argument value has exactly one spelling: the parsed spec is
      // canonicalized and later frozen into segment headers, where "ZSTD(03)" and "ZSTD(3)" must not differ.
      if (_pos - start > 1 && first == '0') {
        throw new IllegalArgumentException(
            "Leading zeros are not allowed in codec argument at position " + start + " in: " + _input);
      }
      return _input.substring(start, _pos);
    }

    private char peek() {
      skipWhitespace();
      return _pos < _input.length() ? _input.charAt(_pos) : '\0';
    }

    private void consume(char expected) {
      skipWhitespace();
      if (_pos >= _input.length()) {
        throw new IllegalArgumentException(
            "Expected '" + expected + "' but reached end of input: " + _input);
      }
      char actual = _input.charAt(_pos);
      if (actual != expected) {
        throw new IllegalArgumentException(
            "Expected '" + expected + "' but got '" + actual + "' at position " + _pos + " in: " + _input);
      }
      _pos++;
    }

    private void skipWhitespace() {
      while (_pos < _input.length() && Character.isWhitespace(_input.charAt(_pos))) {
        _pos++;
      }
    }

    private void expectEof() {
      skipWhitespace();
      if (_pos < _input.length()) {
        throw new IllegalArgumentException(
            "Unexpected trailing content '" + _input.substring(_pos) + "' in: " + _input);
      }
    }

    private static boolean isAsciiIdentifierStart(char c) {
      return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isAsciiIdentifierPart(char c) {
      return isAsciiIdentifierStart(c) || (c >= '0' && c <= '9');
    }
  }
}
