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
import java.util.Collections;
import java.util.List;
import java.util.Locale;


/// Phase-1 (structural) parser for the Codec DSL.
///
/// Accepts the following grammar:
/// ```
///   spec        ::= pipeline | invocation
///   pipeline    ::= "CODEC" "(" invocation ("," invocation)* ")"
///   invocation  ::= NAME
///                 | NAME "(" args ")"
///   args        ::= ε | arg ("," arg)*
///   arg         ::= [0-9]+    // numeric literals; leading +/- signs are rejected in v1
///   NAME        ::= [A-Za-z_][A-Za-z0-9_]*
/// ```
///
/// Examples:
///
/// - `DELTA` → single-stage pipeline with no args
/// - `ZSTD(3)` → single-stage pipeline, arg "3"
/// - `CODEC(DELTA,ZSTD(8))` → two-stage pipeline
///
/// This class performs only structural parsing; semantic validation (valid codec names,
/// argument ranges, type compatibility) is done in a second phase by the registry and
/// pipeline validator.
///
/// This class is stateless and thread-safe.
public final class CodecSpecParser {

  /// The keyword used for multi-stage pipeline notation (e.g. `CODEC(DELTA,ZSTD(3))`).
  ///
  /// This name is permanently reserved — it cannot be registered as a codec name.
  /// See `CodecRegistry.register`.
  public static final String PIPELINE_KEYWORD = "CODEC";

  private CodecSpecParser() {
  }

  /// Parses `spec` into a [CodecPipeline] AST.
  ///
  /// @param spec the codec DSL string; must not be `null` or blank
  /// @return the parsed pipeline
  /// @throws IllegalArgumentException if the spec is syntactically invalid
  public static CodecPipeline parse(String spec) {
    if (spec == null || spec.isBlank()) {
      throw new IllegalArgumentException("codecSpec must not be null or blank");
    }
    String trimmed = spec.trim();
    Parser p = new Parser(trimmed);
    CodecPipeline pipeline = p.parsePipelineOrInvocation();
    p.expectEof();
    return pipeline;
  }

  // -------------------------------------------------------------------------
  // Internal recursive-descent parser
  // -------------------------------------------------------------------------

  private static final class Parser {
    private final String _input;
    private int _pos;

    Parser(String input) {
      _input = input;
      _pos = 0;
    }

    CodecPipeline parsePipelineOrInvocation() {
      String name = parseName();
      if (PIPELINE_KEYWORD.equalsIgnoreCase(name) && peek() == '(') {
        // Multi-stage pipeline: CODEC(stage,stage,...)
        consume('(');
        if (peek() == ')') {
          throw new IllegalArgumentException("CODEC pipeline must have at least one stage in: " + _input);
        }
        List<CodecInvocation> stages = new ArrayList<>();
        stages.add(parseInvocation());
        while (peek() == ',') {
          consume(',');
          stages.add(parseInvocation());
        }
        consume(')');
        return new CodecPipeline(stages);
      }
      // Single invocation; name already consumed, check for optional args
      List<String> args = Collections.emptyList();
      if (peek() == '(') {
        consume('(');
        args = parseArgs();
        consume(')');
      }
      return new CodecPipeline(Collections.singletonList(new CodecInvocation(name.toUpperCase(Locale.ROOT), args)));
    }

    CodecInvocation parseInvocation() {
      String name = parseName().toUpperCase(Locale.ROOT);
      List<String> args = Collections.emptyList();
      if (peek() == '(') {
        consume('(');
        args = parseArgs();
        consume(')');
      }
      return new CodecInvocation(name, args);
    }

    /// Parse NAME = [A-Za-z_][A-Za-z0-9_]*
    String parseName() {
      skipWhitespace();
      if (_pos >= _input.length()) {
        throw new IllegalArgumentException("Expected codec name but reached end of input: " + _input);
      }
      char first = _input.charAt(_pos);
      // ASCII-only identifier ([A-Za-z_][A-Za-z0-9_]*) so the on-disk canonical spec is locale-stable
      // and unambiguous.
      if (!isAsciiIdentifierStart(first)) {
        throw new IllegalArgumentException(
            "Expected codec name starting with [A-Za-z_] at position " + _pos + " in: " + _input);
      }
      int start = _pos++;
      while (_pos < _input.length() && isAsciiIdentifierPart(_input.charAt(_pos))) {
        _pos++;
      }
      return _input.substring(start, _pos);
    }

    private static boolean isAsciiIdentifierStart(char c) {
      return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isAsciiIdentifierPart(char c) {
      return isAsciiIdentifierStart(c) || (c >= '0' && c <= '9');
    }

    /// Parse comma-separated integer args; returns empty list if closing ')' is next.
    List<String> parseArgs() {
      skipWhitespace();
      if (peek() == ')') {
        return Collections.emptyList();
      }
      List<String> args = new ArrayList<>();
      args.add(parseArg());
      while (peek() == ',') {
        consume(',');
        args.add(parseArg());
      }
      return args;
    }

    /// Parse a single arg — currently only integer literals are supported.
    String parseArg() {
      skipWhitespace();
      if (_pos >= _input.length()) {
        throw new IllegalArgumentException("Expected argument but reached end of input: " + _input);
      }
      int start = _pos;
      // Reject leading signs: v1 codec args are unsigned integers; allowing '+3' and '3' to parse
      // separately would produce two distinct canonical strings that compare unequal.
      char first = _input.charAt(_pos);
      if (first == '-' || first == '+') {
        throw new IllegalArgumentException(
            "Leading sign is not allowed in codec argument at position " + start + " in: " + _input);
      }
      // ASCII digits only — `Character.isDigit` accepts non-ASCII digits (e.g. Devanagari) which
      // `Integer.parseInt` then rejects, producing confusing errors. Locking down to '0'..'9'
      // keeps the on-disk canonical spec locale-stable and matches the parser's identifier rule.
      if (first < '0' || first > '9') {
        throw new IllegalArgumentException(
            "Expected integer argument at position " + start + " in: " + _input);
      }
      while (_pos < _input.length()) {
        char c = _input.charAt(_pos);
        if (c < '0' || c > '9') {
          break;
        }
        _pos++;
      }
      return _input.substring(start, _pos).trim();
    }

    char peek() {
      skipWhitespace();
      return _pos < _input.length() ? _input.charAt(_pos) : '\0';
    }

    void consume(char expected) {
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

    void skipWhitespace() {
      while (_pos < _input.length() && Character.isWhitespace(_input.charAt(_pos))) {
        _pos++;
      }
    }

    void expectEof() {
      skipWhitespace();
      if (_pos < _input.length()) {
        throw new IllegalArgumentException(
            "Unexpected trailing content '" + _input.substring(_pos) + "' in: " + _input);
      }
    }
  }
}
