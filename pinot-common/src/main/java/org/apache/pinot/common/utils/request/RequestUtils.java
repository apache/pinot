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
package org.apache.pinot.common.utils.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RequestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestUtils.class);
  private static final JsonNode EMPTY_OBJECT_NODE = new ObjectMapper().createObjectNode();

  private RequestUtils() {
  }

  public static SqlNodeAndOptions parseQuery(String query)
      throws SqlCompilationException {
    return parseQuery(query, EMPTY_OBJECT_NODE);
  }

  public static SqlNodeAndOptions parseQuery(String query, JsonNode request)
      throws SqlCompilationException {
    long parserStartTimeNs = System.nanoTime();
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    setOptions(sqlNodeAndOptions, request);
    sqlNodeAndOptions.setParseTimeNs(System.nanoTime() - parserStartTimeNs);
    return sqlNodeAndOptions;
  }

  /**
   * Sets extra options for the given query.
   */
  @VisibleForTesting
  public static void setOptions(SqlNodeAndOptions sqlNodeAndOptions, JsonNode jsonRequest) {
    Map<String, String> queryOptions = new HashMap<>();
    if (jsonRequest.has(Request.QUERY_OPTIONS)) {
      queryOptions.putAll(getOptionsFromString(jsonRequest.get(Request.QUERY_OPTIONS).asText()));
    }
    if (jsonRequest.has(Request.TRACE) && jsonRequest.get(Request.TRACE).asBoolean()) {
      queryOptions.put(Request.TRACE, "true");
    }
    if (!queryOptions.isEmpty()) {
      LOGGER.debug("Query options are set to: {}", queryOptions);
    }
    // Setting all query options back into SqlNodeAndOptions. The above ordering matters due to priority overwrite rule
    sqlNodeAndOptions.setExtraOptions(queryOptions);
  }

  public static Expression getIdentifierExpression(String identifier) {
    Expression expression = new Expression(ExpressionType.IDENTIFIER);
    expression.setIdentifier(new Identifier(identifier));
    return expression;
  }

  public static Expression getLiteralExpression(SqlLiteral node) {
    Expression expression = new Expression(ExpressionType.LITERAL);
    Literal literal = new Literal();
    if (node instanceof SqlNumericLiteral) {
      BigDecimal bigDecimalValue = node.bigDecimalValue();
      assert bigDecimalValue != null;
      SqlNumericLiteral sqlNumericLiteral = (SqlNumericLiteral) node;
      if (sqlNumericLiteral.isExact() && sqlNumericLiteral.isInteger()) {
        long longValue = bigDecimalValue.longValue();
        if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
          literal.setIntValue((int) longValue);
        } else {
          literal.setLongValue(longValue);
        }
      } else {
        // TODO: Support exact decimal value
        literal.setDoubleValue(bigDecimalValue.doubleValue());
      }
    } else {
      switch (node.getTypeName()) {
        case BOOLEAN:
          literal.setBoolValue(node.booleanValue());
          break;
        case NULL:
          literal.setNullValue(true);
          break;
        default:
          literal.setStringValue(StringUtils.replace(node.toValue(), "''", "'"));
          break;
      }
    }
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression createNewLiteralExpression() {
    Expression expression = new Expression(ExpressionType.LITERAL);
    Literal literal = new Literal();
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression getLiteralExpression(boolean value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBoolValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(int value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setIntValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(long value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setLongValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(float value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setFloatValue(Float.floatToRawIntBits(value));
    return expression;
  }

  public static Expression getLiteralExpression(double value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setDoubleValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(BigDecimal value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBigDecimalValue(BigDecimalUtils.serialize(value));
    return expression;
  }

  public static Expression getLiteralExpression(String value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setStringValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(byte[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBinaryValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(int[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setIntArrayValue(Arrays.stream(value).boxed().collect(Collectors.toList()));
    return expression;
  }

  public static Expression getLiteralExpression(long[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setLongArrayValue(Arrays.stream(value).boxed().collect(Collectors.toList()));
    return expression;
  }

  public static Expression getLiteralExpression(float[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setFloatArrayValue(
        IntStream.range(0, value.length).mapToObj(i -> Float.floatToRawIntBits(value[i])).collect(Collectors.toList()));
    return expression;
  }

  public static Expression getLiteralExpression(double[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setDoubleArrayValue(Arrays.stream(value).boxed().collect(Collectors.toList()));
    return expression;
  }

  public static Expression getLiteralExpression(String[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setStringArrayValue(Arrays.asList(value));
    return expression;
  }

  public static Expression getNullLiteralExpression() {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setNullValue(true);
    return expression;
  }

  public static Expression getLiteralExpression(@Nullable Object object) {
    if (object == null) {
      return getNullLiteralExpression();
    }
    if (object instanceof Boolean) {
      return RequestUtils.getLiteralExpression((boolean) object);
    }
    if (object instanceof Integer) {
      return RequestUtils.getLiteralExpression((int) object);
    }
    if (object instanceof Long) {
      return RequestUtils.getLiteralExpression((long) object);
    }
    if (object instanceof Float) {
      return RequestUtils.getLiteralExpression((float) object);
    }
    if (object instanceof Double) {
      return RequestUtils.getLiteralExpression((double) object);
    }
    if (object instanceof BigDecimal) {
      return RequestUtils.getLiteralExpression((BigDecimal) object);
    }
    if (object instanceof String) {
      return RequestUtils.getLiteralExpression((String) object);
    }
    if (object instanceof byte[]) {
      return RequestUtils.getLiteralExpression((byte[]) object);
    }
    if (object instanceof int[]) {
      return RequestUtils.getLiteralExpression((int[]) object);
    }
    if (object instanceof long[]) {
      return RequestUtils.getLiteralExpression((long[]) object);
    }
    if (object instanceof float[]) {
      return RequestUtils.getLiteralExpression((float[]) object);
    }
    if (object instanceof double[]) {
      return RequestUtils.getLiteralExpression((double[]) object);
    }
    if (object instanceof String[]) {
      return RequestUtils.getLiteralExpression((String[]) object);
    }
    return RequestUtils.getLiteralExpression(object.toString());
  }

  /**
   * Returns the value of the given literal.
   */
  @Nullable
  public static Object getLiteralValue(Literal literal) {
    Literal._Fields type = literal.getSetField();
    switch (type) {
      case NULL_VALUE:
        return null;
      case BOOL_VALUE:
        return literal.getBoolValue();
      case INT_VALUE:
        return literal.getIntValue();
      case LONG_VALUE:
        return literal.getLongValue();
      case FLOAT_VALUE:
        return Float.intBitsToFloat(literal.getFloatValue());
      case DOUBLE_VALUE:
        return literal.getDoubleValue();
      case BIG_DECIMAL_VALUE:
        return BigDecimalUtils.deserialize(literal.getBigDecimalValue());
      case STRING_VALUE:
        return literal.getStringValue();
      case BINARY_VALUE:
        return literal.getBinaryValue();
      case INT_ARRAY_VALUE:
        return literal.getIntArrayValue().stream().mapToInt(Integer::intValue).toArray();
      case LONG_ARRAY_VALUE:
        return literal.getLongArrayValue().stream().mapToLong(Long::longValue).toArray();
      case FLOAT_ARRAY_VALUE:
        List<Integer> floatList = literal.getFloatArrayValue();
        int numFloats = floatList.size();
        float[] floatArray = new float[numFloats];
        for (int i = 0; i < numFloats; i++) {
          floatArray[i] = Float.intBitsToFloat(floatList.get(i));
        }
        return floatArray;
      case DOUBLE_ARRAY_VALUE:
        return literal.getDoubleArrayValue().stream().mapToDouble(Double::doubleValue).toArray();
      case STRING_ARRAY_VALUE:
        return literal.getStringArrayValue().toArray(new String[0]);
      default:
        throw new IllegalStateException("Unsupported field type: " + type);
    }
  }

  /**
   * Returns the string representation of the given literal.
   */
  public static String getLiteralString(Literal literal) {
    Literal._Fields type = literal.getSetField();
    switch (type) {
      case BOOL_VALUE:
        return Boolean.toString(literal.getBoolValue());
      case INT_VALUE:
        return Integer.toString(literal.getIntValue());
      case LONG_VALUE:
        return Long.toString(literal.getLongValue());
      case FLOAT_VALUE:
        return Float.toString(Float.intBitsToFloat(literal.getFloatValue()));
      case DOUBLE_VALUE:
        return Double.toString(literal.getDoubleValue());
      case BIG_DECIMAL_VALUE:
        return BigDecimalUtils.deserialize(literal.getBigDecimalValue()).toPlainString();
      case STRING_VALUE:
        return literal.getStringValue();
      case BINARY_VALUE:
        return BytesUtils.toHexString(literal.getBinaryValue());
      default:
        throw new IllegalStateException("Unsupported string representation of field type: " + type);
    }
  }

  public static String getLiteralString(Expression expression) {
    Literal literal = expression.getLiteral();
    Preconditions.checkArgument(literal != null, "Got non-literal expression: %s", expression);
    return getLiteralString(literal);
  }

  public static Function getFunction(String canonicalName, List<Expression> operands) {
    Function function = new Function(canonicalName);
    function.setOperands(operands);
    return function;
  }

  public static Function getFunction(String canonicalName, Expression operand) {
    // NOTE: Create an ArrayList because we might need to modify the list later
    List<Expression> operands = new ArrayList<>(1);
    operands.add(operand);
    return getFunction(canonicalName, operands);
  }

  public static Function getFunction(String canonicalName, Expression... operands) {
    // NOTE: Create an ArrayList because we might need to modify the list later
    return getFunction(canonicalName, new ArrayList<>(Arrays.asList(operands)));
  }

  public static Expression getFunctionExpression(Function function) {
    Expression expression = new Expression(ExpressionType.FUNCTION);
    expression.setFunctionCall(function);
    return expression;
  }

  public static Expression getFunctionExpression(String canonicalName, List<Expression> operands) {
    return getFunctionExpression(getFunction(canonicalName, operands));
  }

  public static Expression getFunctionExpression(String canonicalName, Expression operand) {
    return getFunctionExpression(getFunction(canonicalName, operand));
  }

  public static Expression getFunctionExpression(String canonicalName, Expression... operands) {
    return getFunctionExpression(getFunction(canonicalName, operands));
  }

  @Deprecated
  public static Expression getFunctionExpression(String canonicalName) {
    assert canonicalName.equalsIgnoreCase(canonicalizeFunctionNamePreservingSpecialKey(canonicalName));
    Expression expression = new Expression(ExpressionType.FUNCTION);
    Function function = new Function(canonicalName);
    expression.setFunctionCall(function);
    return expression;
  }

  /**
   * Converts the function name into its canonical form.
   */
  public static String canonicalizeFunctionName(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  private static final Map<String, String> CANONICAL_NAME_TO_SPECIAL_KEY_MAP;

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (FilterKind filterKind : FilterKind.values()) {
      builder.put(canonicalizeFunctionName(filterKind.name()), filterKind.name());
    }
    CANONICAL_NAME_TO_SPECIAL_KEY_MAP = builder.build();
  }

  /**
   * Converts the function name into its canonical form, but preserving the special keys.
   * - Keep FilterKind.name() as is because we need to read the FilterKind via FilterKind.valueOf().
   */
  public static String canonicalizeFunctionNamePreservingSpecialKey(String functionName) {
    String canonicalName = canonicalizeFunctionName(functionName);
    return CANONICAL_NAME_TO_SPECIAL_KEY_MAP.getOrDefault(canonicalName, canonicalName);
  }

  public static String prettyPrint(@Nullable Expression expression) {
    if (expression == null) {
      return "null";
    }
    if (expression.getIdentifier() != null) {
      return expression.getIdentifier().getName();
    }
    if (expression.getLiteral() != null) {
      return prettyPrint(expression.getLiteral());
    }
    if (expression.getFunctionCall() != null) {
      String res = expression.getFunctionCall().getOperator() + "(";
      boolean isFirstParam = true;
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        if (!isFirstParam) {
          res += ", ";
        } else {
          isFirstParam = false;
        }
        res += prettyPrint(operand);
      }
      res += ")";
      return res;
    }
    throw new IllegalStateException("Unsupported expression type: " + expression.getType());
  }

  public static String prettyPrint(Literal literal) {
    Literal._Fields type = literal.getSetField();
    switch (type) {
      case NULL_VALUE:
        return "null";
      case BOOL_VALUE:
        return Boolean.toString(literal.getBoolValue());
      case INT_VALUE:
        return Integer.toString(literal.getIntValue());
      case LONG_VALUE:
        return Long.toString(literal.getLongValue());
      case FLOAT_VALUE:
        return Float.toString(Float.intBitsToFloat(literal.getFloatValue()));
      case DOUBLE_VALUE:
        return Double.toString(literal.getDoubleValue());
      case BIG_DECIMAL_VALUE:
        return BigDecimalUtils.deserialize(literal.getBigDecimalValue()).toPlainString();
      case STRING_VALUE:
        return "'" + literal.getStringValue() + "'";
      case BINARY_VALUE:
        return "X'" + BytesUtils.toHexString(literal.getBinaryValue()) + "'";
      case INT_ARRAY_VALUE:
        return literal.getIntArrayValue().toString();
      case LONG_ARRAY_VALUE:
        return literal.getLongArrayValue().toString();
      case FLOAT_ARRAY_VALUE:
        return literal.getFloatArrayValue().stream().map(Float::intBitsToFloat).collect(Collectors.toList()).toString();
      case DOUBLE_ARRAY_VALUE:
        return literal.getDoubleArrayValue().toString();
      case STRING_ARRAY_VALUE:
        return literal.getStringArrayValue().stream().map(value -> "'" + value + "'").collect(Collectors.toList())
            .toString();
      default:
        throw new IllegalStateException("Unsupported field type: " + type);
    }
  }

  private static Set<String> getTableNames(DataSource dataSource) {
    if (dataSource.getSubquery() != null) {
      return getTableNames(dataSource.getSubquery());
    } else if (dataSource.isSetJoin()) {
      return ImmutableSet.<String>builder().addAll(getTableNames(dataSource.getJoin().getLeft()))
          .addAll(getTableNames(dataSource.getJoin().getLeft())).build();
    }
    return ImmutableSet.of(dataSource.getTableName());
  }

  public static Set<String> getTableNames(PinotQuery pinotQuery) {
    return getTableNames(pinotQuery.getDataSource());
  }

  @Deprecated
  public static Map<String, String> getOptionsFromJson(JsonNode request, String optionsKey) {
    return getOptionsFromString(request.get(optionsKey).asText());
  }

  public static Map<String, String> getOptionsFromString(String optionStr) {
    return Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(optionStr);
  }
}
