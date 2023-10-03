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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
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
    if (jsonRequest.has(CommonConstants.Broker.Request.DEBUG_OPTIONS)) {
      Map<String, String> debugOptions = RequestUtils.getOptionsFromJson(jsonRequest,
          CommonConstants.Broker.Request.DEBUG_OPTIONS);
      // TODO: remove debug options after releasing 0.11.0.
      if (!debugOptions.isEmpty()) {
        // NOTE: Debug options are deprecated. Put all debug options into query options for backward compatibility.
        LOGGER.debug("Debug options are set to: {}", debugOptions);
        queryOptions.putAll(debugOptions);
      }
    }
    if (jsonRequest.has(CommonConstants.Broker.Request.QUERY_OPTIONS)) {
      Map<String, String> queryOptionsFromJson = RequestUtils.getOptionsFromJson(jsonRequest,
          CommonConstants.Broker.Request.QUERY_OPTIONS);
      queryOptions.putAll(queryOptionsFromJson);
    }
    boolean enableTrace = jsonRequest.has(CommonConstants.Broker.Request.TRACE) && jsonRequest.get(
        CommonConstants.Broker.Request.TRACE).asBoolean();
    if (enableTrace) {
      queryOptions.put(CommonConstants.Broker.Request.TRACE, "true");
    }
    if (!queryOptions.isEmpty()) {
      LOGGER.debug("Query options are set to: {}", queryOptions);
    }
    // TODO: Remove the SQL query options after releasing 0.11.0
    // The query engine will break if these 2 options are missing during version upgrade.
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.GROUP_BY_MODE, CommonConstants.Broker.Request.SQL);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);
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
      // TODO: support different integer and floating point type.
      // Mitigate calcite NPE bug, we need to check if SqlNumericLiteral.getScale() is null before calling
      // SqlNumericLiteral.isInteger(). TODO: Undo this fix once a Calcite release that contains CALCITE-4199 is
      // available and Pinot has been upgraded to use such a release.
      SqlNumericLiteral sqlNumericLiteral = (SqlNumericLiteral) node;
      if (sqlNumericLiteral.getScale() != null && sqlNumericLiteral.isInteger()) {
        literal.setLongValue(node.bigDecimalValue().longValue());
      } else {
        literal.setDoubleValue(node.bigDecimalValue().doubleValue());
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

  public static Expression getLiteralExpression(long value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setLongValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(double value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setDoubleValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(String value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setStringValue(value);
    return expression;
  }

  /**
   * TODO: We should use {@link #getLiteralExpression(ByteArray)} instead of this method to handle binary.
   *
   */
  @Deprecated
  public static Expression getLiteralExpression(byte[] value) {
    Expression expression = createNewLiteralExpression();
    // TODO(After 1.0.0): This is for backward-compatibility, we can set the binary value directly instead of
    //  converting it to hex string after the next released version.
    expression.getLiteral().setStringValue(BytesUtils.toHexString(value));
    return expression;
  }

  public static Expression getLiteralExpression(ByteArray value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBinaryValue(value.getBytes());
    return expression;
  }

  public static Expression getLiteralExpression(BigDecimal value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBigDecimalValue(BigDecimalUtils.serialize(value));
    return expression;
  }

  public static Expression getNullLiteralExpression() {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setNullValue(true);
    return expression;
  }

  /**
   * The input object should only be internal data type representation.
   */
  public static Expression getLiteralExpression(Object object) {
    if (object == null) {
      return getNullLiteralExpression();
    }
    if (object instanceof Integer || object instanceof Long) {
      return RequestUtils.getLiteralExpression(((Number) object).longValue());
    }
    if (object instanceof Float) {
      // We need to use Double.parseDouble(object.toString()) instead of ((Number) object).doubleValue()
      // or ((Float) object).doubleValue() because the latter two will return slightly different values
      // For example, if object is 0.06f, Double.parseDouble(object.toString()) will return 0.06, while
      // ((Number) object).doubleValue() or ((Float) object).doubleValue() will return 0.05999999865889549
      return RequestUtils.getLiteralExpression(Double.parseDouble(object.toString()));
    }
    if (object instanceof Double) {
      return RequestUtils.getLiteralExpression(((Double) object).doubleValue());
    }
    // byte[] is not an internal data type, put it here for backward compatibility, we should use ByteArray instead.
    if (object instanceof byte[]) {
      return RequestUtils.getLiteralExpression((byte[]) object);
    }
    if (object instanceof ByteArray) {
      return RequestUtils.getLiteralExpression((ByteArray) object);
    }
    if (object instanceof Boolean) {
      return RequestUtils.getLiteralExpression(((Boolean) object).booleanValue());
    }
    if (object instanceof String) {
      return RequestUtils.getLiteralExpression((String) object);
    }
    throw new UnsupportedOperationException("Unsupported data type: " + object.getClass() + " with value: " + object);
  }

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
    CANONICAL_NAME_TO_SPECIAL_KEY_MAP = new HashMap<>();
    for (FilterKind filterKind : FilterKind.values()) {
      CANONICAL_NAME_TO_SPECIAL_KEY_MAP.put(canonicalizeFunctionName(filterKind.name()), filterKind.name());
    }
    CANONICAL_NAME_TO_SPECIAL_KEY_MAP.put("stdistance", "st_distance");
  }

  /**
   * Converts the function name into its canonical form, but preserving the special keys.
   * - Keep FilterKind.name() as is because we need to read the FilterKind via FilterKind.valueOf().
   * - Keep ST_Distance as is because we use exact match when applying geo-spatial index up to release 0.10.0.
   * TODO: Remove the ST_Distance special handling after releasing 0.11.0.
   */
  public static String canonicalizeFunctionNamePreservingSpecialKey(String functionName) {
    String canonicalName = canonicalizeFunctionName(functionName);
    return CANONICAL_NAME_TO_SPECIAL_KEY_MAP.getOrDefault(canonicalName, canonicalName);
  }

  public static String prettyPrint(Expression expression) {
    if (expression == null) {
      return "null";
    }
    if (expression.getIdentifier() != null) {
      return expression.getIdentifier().getName();
    }
    if (expression.getLiteral() != null) {
      if (expression.getLiteral().isSetLongValue()) {
        return Long.toString(expression.getLiteral().getLongValue());
      }
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
    return null;
  }

  private static Set<String> getTableNames(DataSource dataSource) {
    if (dataSource.getSubquery() != null) {
      return getTableNames(dataSource.getSubquery());
    } else if (dataSource.isSetJoin()) {
      return ImmutableSet.<String>builder()
          .addAll(getTableNames(dataSource.getJoin().getLeft()))
          .addAll(getTableNames(dataSource.getJoin().getLeft())).build();
    }
    return ImmutableSet.of(dataSource.getTableName());
  }

  public static Set<String> getTableNames(PinotQuery pinotQuery) {
    return getTableNames(pinotQuery.getDataSource());
  }

  public static Map<String, String> getOptionsFromJson(JsonNode request, String optionsKey) {
    return getOptionsFromString(request.get(optionsKey).asText());
  }

  public static Map<String, String> getOptionsFromString(String optionStr) {
    return Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(optionStr);
  }
}
