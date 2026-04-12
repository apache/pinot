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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.function.ExecutableFunctionEvaluator;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;


/**
 * Compiles a restricted partition-function expression into a typed {@link PartitionPipeline} backed by deterministic
 * scalar functions.
 */
public final class PartitionFunctionExprCompiler {
  private static final int MAX_EXPRESSION_LENGTH = 256;
  private static final int MAX_DEPTH = 8;
  private static final int MAX_ARGUMENTS = 32;
  private static final PartitionScalarFunctionResolver FUNCTION_RESOLVER = loadFunctionResolver();

  private PartitionFunctionExprCompiler() {
  }

  public static PartitionPipeline compile(String rawColumn, String functionExpr) {
    return compile(rawColumn, functionExpr, PartitionIntNormalizer.POSITIVE_MODULO);
  }

  public static PartitionPipeline compile(String rawColumn, String functionExpr,
      @Nullable PartitionIntNormalizer partitionIdNormalizer) {
    Preconditions.checkArgument(hasText(rawColumn), "Raw column must be configured");
    Preconditions.checkArgument(hasText(functionExpr), "'functionExpr' must be configured");
    Preconditions.checkArgument(functionExpr.length() <= MAX_EXPRESSION_LENGTH,
        "'functionExpr' must be <= %s characters", MAX_EXPRESSION_LENGTH);

    Parser parser = new Parser(functionExpr);
    Node root = parser.parse();
    List<PartitionStep> steps = new ArrayList<>();
    CompileResult result = compile(rawColumn, root, steps, 0);
    Preconditions.checkArgument(result._dynamic,
        "Partition function expression must reference partition column '%s'", rawColumn);
    PartitionIntNormalizer effectivePartitionIdNormalizer =
        result._outputType.isIntegral() ? partitionIdNormalizer : null;
    return new PartitionPipeline(rawColumn, PartitionValueType.STRING, result._outputType, result._canonicalExpr,
        effectivePartitionIdNormalizer, steps, result._rootNode);
  }

  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, String functionExpr,
      int numPartitions) {
    return compilePartitionFunction(rawColumn, functionExpr, numPartitions, null);
  }

  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, String functionExpr,
      int numPartitions, @Nullable String partitionIdNormalizer) {
    PartitionIntNormalizer configuredPartitionIdNormalizer = partitionIdNormalizer != null
        ? PartitionIntNormalizer.fromConfigString(partitionIdNormalizer)
        : PartitionIntNormalizer.fromConfigString(ColumnPartitionConfig.PARTITION_ID_NORMALIZER_POSITIVE_MODULO);
    return new PartitionPipelineFunction(compile(rawColumn, functionExpr, configuredPartitionIdNormalizer),
        numPartitions);
  }

  private static CompileResult compile(String rawColumn, Node node, List<PartitionStep> steps, int depth) {
    Preconditions.checkArgument(depth <= MAX_DEPTH,
        "Partition function expression depth exceeds the maximum of %s", MAX_DEPTH);
    if (node instanceof IdentifierNode) {
      IdentifierNode identifierNode = (IdentifierNode) node;
      Preconditions.checkArgument(identifierNode._name.equals(rawColumn),
          "Partition function expression must reference only partition column '%s', got '%s'",
          rawColumn, identifierNode._name);
      return CompileResult.dynamic(PartitionValueType.STRING, rawColumn,
          new ExecutableFunctionEvaluator.ColumnNode(rawColumn, 0));
    }
    if (node instanceof LiteralNode) {
      LiteralNode literalNode = (LiteralNode) node;
      return CompileResult.literal(literalNode._value.getType(), literalNode._canonicalForm, literalNode._value,
          new ExecutableFunctionEvaluator.ConstantNode(literalNode._value.toObject()));
    }

    FunctionNode functionNode = (FunctionNode) node;
    List<CompileResult> arguments = new ArrayList<>(functionNode._arguments.size());
    int dynamicCount = 0;
    int dynamicIndex = -1;
    for (int i = 0; i < functionNode._arguments.size(); i++) {
      CompileResult argument = compile(rawColumn, functionNode._arguments.get(i), steps, depth + 1);
      Preconditions.checkArgument(argument._dynamic || argument._literalConstant,
          "Partition function expression only supports literal constants, got function subexpression: %s",
          argument._canonicalExpr);
      arguments.add(argument);
      if (argument._dynamic) {
        dynamicCount++;
        dynamicIndex = i;
      }
    }
    Preconditions.checkArgument(dynamicCount <= 1,
        "Partition function expression must reference partition column '%s' through a single argument chain",
        rawColumn);

    String displayName = functionNode._name.toLowerCase(Locale.ROOT);
    PartitionScalarFunctionResolver.ResolvedFunction resolvedFunction =
        FUNCTION_RESOLVER.resolve(displayName, toResolverArguments(arguments));
    String canonicalExpr = toCanonicalExpr(displayName, arguments);
    if (!resolvedFunction.isDynamic()) {
      PartitionValue constantValue = resolvedFunction.invoke(null);
      return CompileResult.constant(resolvedFunction.getOutputType(), canonicalExpr, constantValue,
          new ExecutableFunctionEvaluator.ConstantNode(constantValue.toObject()));
    }

    steps.add(new PartitionStep(displayName, arguments.get(dynamicIndex)._outputType, resolvedFunction.getOutputType(),
        resolvedFunction::invoke));
    ExecutableFunctionEvaluator.ExecutableNode[] dynamicArgumentNodes = dynamicCount == 1
        ? new ExecutableFunctionEvaluator.ExecutableNode[]{arguments.get(dynamicIndex)._rootNode}
        : new ExecutableFunctionEvaluator.ExecutableNode[0];
    return CompileResult.dynamic(resolvedFunction.getOutputType(), canonicalExpr,
        new ExecutableFunctionEvaluator.FunctionNode(displayName, new PartitionBoundFunctionInvoker(resolvedFunction),
            dynamicArgumentNodes));
  }

  private static List<PartitionScalarFunctionResolver.Argument> toResolverArguments(List<CompileResult> arguments) {
    List<PartitionScalarFunctionResolver.Argument> resolverArguments = new ArrayList<>(arguments.size());
    for (CompileResult argument : arguments) {
      resolverArguments.add(argument._dynamic ? PartitionScalarFunctionResolver.Argument.dynamic(argument._outputType)
          : PartitionScalarFunctionResolver.Argument.constant(
              Preconditions.checkNotNull(argument._constantValue, "Constant argument must be configured")));
    }
    return resolverArguments;
  }

  private static PartitionScalarFunctionResolver loadFunctionResolver() {
    List<PartitionScalarFunctionResolver> resolvers = new ArrayList<>();
    for (PartitionScalarFunctionResolver resolver : ServiceLoader.load(PartitionScalarFunctionResolver.class)) {
      resolvers.add(resolver);
    }
    Preconditions.checkState(!resolvers.isEmpty(),
        "No PartitionScalarFunctionResolver implementation found on the classpath");
    Preconditions.checkState(resolvers.size() == 1,
        "Expected exactly 1 PartitionScalarFunctionResolver implementation but found %s: %s", resolvers.size(),
        resolvers);
    return resolvers.get(0);
  }

  private static String toCanonicalExpr(String functionName, List<CompileResult> arguments) {
    StringBuilder builder = new StringBuilder(functionName).append('(');
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(arguments.get(i)._canonicalExpr);
    }
    return builder.append(')').toString();
  }

  private static boolean hasText(@Nullable String value) {
    return value != null && !value.trim().isEmpty();
  }

  private abstract static class Node {
  }

  private static final class IdentifierNode extends Node {
    private final String _name;

    private IdentifierNode(String name) {
      _name = name;
    }
  }

  private static final class LiteralNode extends Node {
    private final PartitionValue _value;
    private final String _canonicalForm;

    private LiteralNode(PartitionValue value, String canonicalForm) {
      _value = value;
      _canonicalForm = canonicalForm;
    }
  }

  private static final class FunctionNode extends Node {
    private final String _name;
    private final List<Node> _arguments;

    private FunctionNode(String name, List<Node> arguments) {
      _name = name;
      _arguments = arguments;
    }
  }

  private static final class CompileResult {
    private final PartitionValueType _outputType;
    private final String _canonicalExpr;
    private final boolean _dynamic;
    private final boolean _literalConstant;
    private final ExecutableFunctionEvaluator.ExecutableNode _rootNode;
    @Nullable
    private final PartitionValue _constantValue;

    private CompileResult(PartitionValueType outputType, String canonicalExpr, boolean dynamic, boolean literalConstant,
        @Nullable PartitionValue constantValue, ExecutableFunctionEvaluator.ExecutableNode rootNode) {
      _outputType = outputType;
      _canonicalExpr = canonicalExpr;
      _dynamic = dynamic;
      _literalConstant = literalConstant;
      _constantValue = constantValue;
      _rootNode = rootNode;
    }

    private static CompileResult dynamic(PartitionValueType outputType, String canonicalExpr,
        ExecutableFunctionEvaluator.ExecutableNode rootNode) {
      return new CompileResult(outputType, canonicalExpr, true, false, null, rootNode);
    }

    private static CompileResult literal(PartitionValueType outputType, String canonicalExpr, PartitionValue value,
        ExecutableFunctionEvaluator.ExecutableNode rootNode) {
      return new CompileResult(outputType, canonicalExpr, false, true, value, rootNode);
    }

    private static CompileResult constant(PartitionValueType outputType, String canonicalExpr, PartitionValue value,
        ExecutableFunctionEvaluator.ExecutableNode rootNode) {
      return new CompileResult(outputType, canonicalExpr, false, false, value, rootNode);
    }
  }

  private static final class PartitionBoundFunctionInvoker implements ExecutableFunctionEvaluator.Invoker {
    private final PartitionScalarFunctionResolver.ResolvedFunction _resolvedFunction;

    private PartitionBoundFunctionInvoker(PartitionScalarFunctionResolver.ResolvedFunction resolvedFunction) {
      _resolvedFunction = resolvedFunction;
    }

    @Override
    public Object invoke(Object[] arguments) {
      Preconditions.checkState(arguments.length <= 1,
          "Partition expression runtime expects at most one dynamic argument, got: %s", arguments.length);
      PartitionValue dynamicInput = arguments.length == 0 ? null : PartitionValue.fromObject(arguments[0]);
      return _resolvedFunction.invoke(dynamicInput).toObject();
    }
  }

  private static final class Parser {
    private final String _expression;
    private int _index;

    private Parser(String expression) {
      _expression = expression;
    }

    private Node parse() {
      skipWhitespace();
      Node node = parseTerm();
      skipWhitespace();
      if (_index != _expression.length()) {
        throw error("Unexpected trailing content");
      }
      return node;
    }

    private Node parseTerm() {
      skipWhitespace();
      if (_index >= _expression.length()) {
        throw error("Unexpected end of expression");
      }
      char character = _expression.charAt(_index);
      if (character == '\'') {
        return parseStringLiteral();
      }
      if (isNumericStart(character)) {
        return parseNumericLiteral();
      }
      String identifier = parseIdentifier();
      skipWhitespace();
      if (_index < _expression.length() && _expression.charAt(_index) == '(') {
        _index++;
        skipWhitespace();
        List<Node> arguments = new ArrayList<>();
        if (_index < _expression.length() && _expression.charAt(_index) == ')') {
          _index++;
          return new FunctionNode(identifier, arguments);
        }
        while (true) {
          arguments.add(parseTerm());
          Preconditions.checkArgument(arguments.size() <= MAX_ARGUMENTS,
              "Partition function expression cannot have more than %s arguments", MAX_ARGUMENTS);
          skipWhitespace();
          if (_index >= _expression.length()) {
            throw error("Expected ')'");
          }
          char delimiter = _expression.charAt(_index);
          if (delimiter == ',') {
            _index++;
            skipWhitespace();
            continue;
          }
          if (delimiter == ')') {
            _index++;
            return new FunctionNode(identifier, arguments);
          }
          throw error("Expected ',' or ')'");
        }
      }
      return new IdentifierNode(identifier);
    }

    private Node parseStringLiteral() {
      _index++;
      StringBuilder builder = new StringBuilder();
      while (_index < _expression.length()) {
        char character = _expression.charAt(_index++);
        if (character == '\'') {
          if (_index < _expression.length() && _expression.charAt(_index) == '\'') {
            builder.append('\'');
            _index++;
            continue;
          }
          return new LiteralNode(PartitionValue.stringValue(builder.toString()), quote(builder.toString()));
        }
        builder.append(character);
      }
      throw error("Unterminated string literal");
    }

    private Node parseNumericLiteral() {
      int start = _index;
      if (_expression.charAt(_index) == '+' || _expression.charAt(_index) == '-') {
        _index++;
      }
      boolean sawDecimal = false;
      while (_index < _expression.length()) {
        char character = _expression.charAt(_index);
        if (Character.isDigit(character)) {
          _index++;
          continue;
        }
        if (character == '.') {
          sawDecimal = true;
          _index++;
          continue;
        }
        if (character == 'e' || character == 'E') {
          sawDecimal = true;
          _index++;
          if (_index < _expression.length()
              && (_expression.charAt(_index) == '+' || _expression.charAt(_index) == '-')) {
            _index++;
          }
          continue;
        }
        break;
      }
      String token = _expression.substring(start, _index);
      try {
        if (sawDecimal) {
          return new LiteralNode(PartitionValue.doubleValue(Double.parseDouble(token)), token);
        }
        return new LiteralNode(PartitionValue.longValue(Long.parseLong(token)), token);
      } catch (NumberFormatException e) {
        throw error("Invalid numeric literal: " + token);
      }
    }

    private String parseIdentifier() {
      Preconditions.checkArgument(_index < _expression.length() && isIdentifierStart(_expression.charAt(_index)),
          "Expected identifier at position %s", _index);
      int start = _index++;
      while (_index < _expression.length() && isIdentifierPart(_expression.charAt(_index))) {
        _index++;
      }
      return _expression.substring(start, _index);
    }

    private void skipWhitespace() {
      while (_index < _expression.length() && Character.isWhitespace(_expression.charAt(_index))) {
        _index++;
      }
    }

    private boolean isNumericStart(char character) {
      if (Character.isDigit(character)) {
        return true;
      }
      if ((character == '+' || character == '-') && _index + 1 < _expression.length()) {
        return Character.isDigit(_expression.charAt(_index + 1));
      }
      return false;
    }

    private boolean isIdentifierStart(char character) {
      return Character.isLetter(character) || character == '_';
    }

    private boolean isIdentifierPart(char character) {
      return Character.isLetterOrDigit(character) || character == '_';
    }

    private IllegalArgumentException error(String message) {
      return new IllegalArgumentException(
          String.format("%s at position %s in partition function expression '%s'", message, _index, _expression));
    }
  }

  private static String quote(String value) {
    return '\'' + value.replace("'", "''") + '\'';
  }
}
