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
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
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

  public static Literal getNullLiteral() {
    return Literal.nullValue(true);
  }

  public static Literal getLiteral(boolean value) {
    return Literal.boolValue(value);
  }

  public static Literal getLiteral(int value) {
    return Literal.intValue(value);
  }

  public static Literal getLiteral(long value) {
    return Literal.longValue(value);
  }

  public static Literal getLiteral(float value) {
    return Literal.floatValue(Float.floatToRawIntBits(value));
  }

  public static Literal getLiteral(double value) {
    return Literal.doubleValue(value);
  }

  public static Literal getLiteral(BigDecimal value) {
    return Literal.bigDecimalValue(BigDecimalUtils.serialize(value));
  }

  public static Literal getLiteral(String value) {
    return Literal.stringValue(value);
  }

  public static Literal getLiteral(byte[] value) {
    return Literal.binaryValue(value);
  }

  public static Literal getLiteral(int[] value) {
    return Literal.intArrayValue(IntArrayList.wrap(value));
  }

  public static Literal getLiteral(long[] value) {
    return Literal.longArrayValue(LongArrayList.wrap(value));
  }

  public static Literal getLiteral(float[] value) {
    IntArrayList intBitsList = new IntArrayList(value.length);
    for (float floatValue : value) {
      intBitsList.add(Float.floatToRawIntBits(floatValue));
    }
    return Literal.floatArrayValue(intBitsList);
  }

  public static Literal getLiteral(double[] value) {
    return Literal.doubleArrayValue(DoubleArrayList.wrap(value));
  }

  public static Literal getLiteral(String[] value) {
    return Literal.stringArrayValue(Arrays.asList(value));
  }

  public static Literal getLiteral(@Nullable Object object) {
    if (object == null) {
      return getNullLiteral();
    }
    if (object instanceof Boolean) {
      return getLiteral((boolean) object);
    }
    if (object instanceof Integer) {
      return getLiteral((int) object);
    }
    if (object instanceof Long) {
      return getLiteral((long) object);
    }
    if (object instanceof Float) {
      return getLiteral((float) object);
    }
    if (object instanceof Double) {
      return getLiteral((double) object);
    }
    if (object instanceof BigDecimal) {
      return getLiteral((BigDecimal) object);
    }
    if (object instanceof Timestamp) {
      return getLiteral(((Timestamp) object).getTime());
    }
    if (object instanceof String) {
      return getLiteral((String) object);
    }
    if (object instanceof byte[]) {
      return getLiteral((byte[]) object);
    }
    if (object instanceof int[]) {
      return getLiteral((int[]) object);
    }
    if (object instanceof long[]) {
      return getLiteral((long[]) object);
    }
    if (object instanceof float[]) {
      return getLiteral((float[]) object);
    }
    if (object instanceof double[]) {
      return getLiteral((double[]) object);
    }
    if (object instanceof String[]) {
      return getLiteral((String[]) object);
    }
    return getLiteral(object.toString());
  }

  public static Literal getLiteral(SqlLiteral node) {
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
    return literal;
  }

  public static Expression getLiteralExpression(Literal literal) {
    Expression expression = new Expression(ExpressionType.LITERAL);
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression getNullLiteralExpression() {
    return getLiteralExpression(getNullLiteral());
  }

  public static Expression getLiteralExpression(boolean value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(int value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(long value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(float value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(double value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(BigDecimal value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(String value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(byte[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(int[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(long[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(float[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(double[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(String[] value) {
    return getLiteralExpression(getLiteral(value));
  }

  public static Expression getLiteralExpression(SqlLiteral node) {
    return getLiteralExpression(getLiteral(node));
  }

  public static Expression getLiteralExpression(@Nullable Object object) {
    return getLiteralExpression(getLiteral(object));
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
        return getIntArrayValue(literal);
      case LONG_ARRAY_VALUE:
        return getLongArrayValue(literal);
      case FLOAT_ARRAY_VALUE:
        return getFloatArrayValue(literal);
      case DOUBLE_ARRAY_VALUE:
        return getDoubleArrayValue(literal);
      case STRING_ARRAY_VALUE:
        return getStringArrayValue(literal);
      default:
        throw new IllegalStateException("Unsupported field type: " + type);
    }
  }

  public static int[] getIntArrayValue(Literal literal) {
    List<Integer> list = literal.getIntArrayValue();
    int size = list.size();
    int[] array = new int[size];
    for (int i = 0; i < size; i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  public static long[] getLongArrayValue(Literal literal) {
    List<Long> list = literal.getLongArrayValue();
    int size = list.size();
    long[] array = new long[size];
    for (int i = 0; i < size; i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  public static float[] getFloatArrayValue(Literal literal) {
    List<Integer> list = literal.getFloatArrayValue();
    int size = list.size();
    float[] array = new float[size];
    for (int i = 0; i < size; i++) {
      array[i] = Float.intBitsToFloat(list.get(i));
    }
    return array;
  }

  public static double[] getDoubleArrayValue(Literal literal) {
    List<Double> list = literal.getDoubleArrayValue();
    int size = list.size();
    double[] array = new double[size];
    for (int i = 0; i < size; i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  public static String[] getStringArrayValue(Literal literal) {
    return literal.getStringArrayValue().toArray(new String[0]);
  }

  public static Pair<ColumnDataType, Object> getLiteralTypeAndValue(Literal literal) {
    Literal._Fields type = literal.getSetField();
    switch (type) {
      case NULL_VALUE:
        return Pair.of(ColumnDataType.UNKNOWN, null);
      case BOOL_VALUE:
        return Pair.of(ColumnDataType.BOOLEAN, literal.getBoolValue());
      case INT_VALUE:
        return Pair.of(ColumnDataType.INT, literal.getIntValue());
      case LONG_VALUE:
        return Pair.of(ColumnDataType.LONG, literal.getLongValue());
      case FLOAT_VALUE:
        return Pair.of(ColumnDataType.FLOAT, Float.intBitsToFloat(literal.getFloatValue()));
      case DOUBLE_VALUE:
        return Pair.of(ColumnDataType.DOUBLE, literal.getDoubleValue());
      case BIG_DECIMAL_VALUE:
        return Pair.of(ColumnDataType.BIG_DECIMAL, BigDecimalUtils.deserialize(literal.getBigDecimalValue()));
      case STRING_VALUE:
        return Pair.of(ColumnDataType.STRING, literal.getStringValue());
      case BINARY_VALUE:
        return Pair.of(ColumnDataType.BYTES, literal.getBinaryValue());
      case INT_ARRAY_VALUE:
        return Pair.of(ColumnDataType.INT_ARRAY, getIntArrayValue(literal));
      case LONG_ARRAY_VALUE:
        return Pair.of(ColumnDataType.LONG_ARRAY, getLongArrayValue(literal));
      case FLOAT_ARRAY_VALUE:
        return Pair.of(ColumnDataType.FLOAT_ARRAY, getFloatArrayValue(literal));
      case DOUBLE_ARRAY_VALUE:
        return Pair.of(ColumnDataType.DOUBLE_ARRAY, getDoubleArrayValue(literal));
      case STRING_ARRAY_VALUE:
        return Pair.of(ColumnDataType.STRING_ARRAY, getStringArrayValue(literal));
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
    if (dataSource == null) {
      return null;
    } else if (dataSource.getSubquery() != null) {
      return getTableNames(dataSource.getSubquery());
    } else if (dataSource.isSetJoin()) {
      return ImmutableSet.<String>builder().addAll(getTableNames(dataSource.getJoin().getLeft()))
          .addAll(getTableNames(dataSource.getJoin().getRight())).build();
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

  public static void applyTimestampIndex(Expression expression, PinotQuery query) {
    applyTimestampIndex(expression, query, timeColumnWithGranularity -> true);
  }

  public static void applyTimestampIndex(
      Expression expression, PinotQuery query, Predicate<String> timeColumnWithGranularityPredicate
  ) {
    if (!expression.isSetFunctionCall()) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (!function.getOperator().equalsIgnoreCase("datetrunc")) {
      return;
    }
    String granularString = function.getOperands().get(0).getLiteral().getStringValue().toUpperCase();
    Expression timeExpression = function.getOperands().get(1);
    if (((function.getOperandsSize() == 2) || (function.getOperandsSize() == 3 && "MILLISECONDS".equalsIgnoreCase(
        function.getOperands().get(2).getLiteral().getStringValue()))) && TimestampIndexUtils.isValidGranularity(
        granularString) && timeExpression.getIdentifier() != null) {
      String timeColumn = timeExpression.getIdentifier().getName();
      String timeColumnWithGranularity = TimestampIndexUtils.getColumnWithGranularity(timeColumn, granularString);

      if (timeColumnWithGranularityPredicate.test(timeColumnWithGranularity)) {
        query.putToExpressionOverrideHints(expression, getIdentifierExpression(timeColumnWithGranularity));
      }
    }
  }
}
