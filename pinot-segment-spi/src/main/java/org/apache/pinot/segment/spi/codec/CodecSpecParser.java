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
///
/// The grammar is function-call shaped, so reusing Pinot's SQL expression parsing is a natural
/// question. `CalciteSqlParser` is not reachable from here: it lives in `pinot-common`, which
/// already depends on this module, so calling into it would close a module cycle. Calcite's own
/// `SqlParser` is on the classpath — this module uses Calcite for type inference — but is
/// deliberately not used either:
///
///   - The canonical form produced here is frozen into segment headers and compared by string
///     equality to detect rewrites. Delegating canonicalization would tie on-disk segment
///     compatibility to Calcite's identifier-casing, quoting, and literal-formatting rules across
///     Calcite upgrades.
///   - A SQL parser accepts far more than this grammar allows — arithmetic, nested calls, string
///     literals, aliases, qualified names. The node-tree rejection logic needed to narrow it back
///     down would exceed this parser in size, and would fail open as new node types appear in
///     later Calcite versions, in a config path that otherwise fails closed.
///   - Parsing runs on every table-config deserialization, from the `ForwardIndexConfig`
///     constructor, on controller, server, and minion.
///
/// The cost of that choice is a narrow argument grammar: unsigned integers only, so neither
/// negative arguments (such as ZSTD fast-mode levels) nor keyword arguments are expressible.
/// Widening this grammar later stays a contained change; removing a Calcite dependency from
/// segment headers would not be.
public final class CodecSpecParser {
  private CodecSpecParser() {
  }

  public static final int MAX_SPEC_LENGTH = CodecDslSyntax.MAX_SPEC_LENGTH;
  public static final int MAX_PIPELINE_STAGES = CodecDslSyntax.MAX_PIPELINE_STAGES;
  public static final int MAX_IDENTIFIER_LENGTH = CodecDslSyntax.MAX_IDENTIFIER_LENGTH;
  public static final int MAX_ARGS_PER_INVOCATION = CodecDslSyntax.MAX_ARGS_PER_INVOCATION;
  public static final int MAX_ARGUMENT_LENGTH = CodecDslSyntax.MAX_ARGUMENT_LENGTH;

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
      if (CodecDslSyntax.isRemovedWrapperName(name)) {
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
      if (!CodecDslSyntax.isAsciiIdentifierStart(first)) {
        throw new IllegalArgumentException(
            "Expected codec name starting with [A-Za-z_] at position " + _pos + " in: " + _input);
      }
      int start = _pos++;
      while (_pos < _input.length() && CodecDslSyntax.isAsciiIdentifierPart(_input.charAt(_pos))) {
        _pos++;
      }
      String name = _input.substring(start, _pos);
      CodecDslSyntax.validateName(name);
      return name;
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
      if (!CodecDslSyntax.isAsciiDigit(first)) {
        throw new IllegalArgumentException("Expected integer argument at position " + start + " in: " + _input);
      }
      while (_pos < _input.length()) {
        char c = _input.charAt(_pos);
        if (!CodecDslSyntax.isAsciiDigit(c)) {
          break;
        }
        _pos++;
      }
      String argument = _input.substring(start, _pos);
      CodecDslSyntax.validateArgument(argument);
      return argument;
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
  }
}
