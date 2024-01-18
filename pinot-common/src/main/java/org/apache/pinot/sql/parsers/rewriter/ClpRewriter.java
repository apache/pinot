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
package org.apache.pinot.sql.parsers.rewriter;

import com.yscope.clp.compressorfrontend.AbstractClpEncodedSubquery.VariableWildcardQuery;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.ByteSegment;
import com.yscope.clp.compressorfrontend.EightByteClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.EightByteClpWildcardQueryEncoder;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Query rewriter to rewrite CLP-related functions: clpDecode and clpMatch.
 * <ul>
 *   <li>clpDecode rewrites the query so users can pass in the name of a CLP-encoded column group instead of the names
 *   of all the columns in the group.</li>
 *   <li>clpMatch rewrites a wildcard query into a SQL boolean expression that implement's CLP's query processing
 *   logic.</li>
 * </ul>
 * <p>
 * clpDecode usage:
 * <pre>
 *   clpDecode("columnGroupName"[, defaultValue])
 * </pre>
 * will be rewritten to:
 * <pre>
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars"[,
 *   defaultValue])
 * </pre>
 * The "defaultValue" is optional. See
 * {@link org.apache.pinot.core.operator.transform.function.CLPDecodeTransformFunction} for its description.
 * <p>
 * Sample queries:
 * <pre>
 *   SELECT clpDecode("message") FROM table
 *   SELECT clpDecode("message", 'null') FROM table
 * </pre>
 * See {@link org.apache.pinot.core.operator.transform.function.CLPDecodeTransformFunction} for details about the
 * underlying clpDecode transformer.
 * <p>
 * clpMatch usage:
 * <pre>
 *   clpMatch("columnGroupName", 'wildcardQuery')
 * </pre>
 * OR
 * <pre>
 *   clpMatch("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars",
 *            'wildcardQuery')
 * </pre>
 * <p>
 * Sample queries:
 * <pre>
 *   SELECT * FROM table WHERE clpMatch(message, '* job1 failed *')
 *   SELECT * FROM table WHERE clpMatch(message_logtype, message_dictionaryVars, message_encodedVars, '* job1 failed *')
 * </pre>
 */
public class ClpRewriter implements QueryRewriter {
  public static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  public static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  public static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  private static final String _CLPDECODE_LOWERCASE_TRANSFORM_NAME =
      TransformFunctionType.CLP_DECODE.getName().toLowerCase();
  private static final String _REGEXP_LIKE_LOWERCASE_FUNCTION_NAME = Predicate.Type.REGEXP_LIKE.name();
  private static final char[] _NON_WILDCARD_REGEX_META_CHARACTERS =
      {'^', '$', '.', '{', '}', '[', ']', '(', ')', '+', '|', '<', '>', '-', '/', '=', '!'};
  private static final String _CLPMATCH_LOWERCASE_FUNCTION_NAME = "clpmatch";

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectExpressions = pinotQuery.getSelectList();
    if (null != selectExpressions) {
      for (Expression e : selectExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    List<Expression> groupByExpressions = pinotQuery.getGroupByList();
    if (null != groupByExpressions) {
      for (Expression e : groupByExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (null != orderByExpressions) {
      for (Expression e : orderByExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    tryRewritingExpression(pinotQuery.getFilterExpression(), true);
    tryRewritingExpression(pinotQuery.getHavingExpression(), true);
    return pinotQuery;
  }

  /**
   * Rewrites any instances of clpDecode or clpMatch in the given expression
   * @param expression Expression which may contain instances of clpDecode
   * @param isFilterExpression Whether the root-level expression (not an expression from a recursive step) is a
   *                           filter expression.
   */
  private void tryRewritingExpression(Expression expression, boolean isFilterExpression) {
    if (null == expression) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (null == function) {
      return;
    }

    String functionName = function.getOperator();
    if (functionName.equals(_CLPDECODE_LOWERCASE_TRANSFORM_NAME)) {
      rewriteCLPDecodeFunction(expression);
      return;
    }

    if (functionName.equals(_CLPMATCH_LOWERCASE_FUNCTION_NAME)) {
      if (!isFilterExpression) {
        throw new SqlCompilationException(
            _CLPMATCH_LOWERCASE_FUNCTION_NAME + " cannot be used outside filter expressions.");
      }
      rewriteClpMatchFunction(expression);
      return;
    }

    // Work around https://github.com/apache/pinot/issues/10478
    if (isClpMatchEqualsFunctionCall(function)) {
      replaceClpMatchEquals(expression, function);
      return;
    }

    if (isInvertedClpMatchEqualsFunctionCall(function)) {
      // Replace `NOT clpMatch(...) = true` with the boolean expression equivalent to `NOT clpMatch(...)`
      List<Expression> operands = function.getOperands();
      Expression op0 = operands.get(0);
      Function f = op0.getFunctionCall();
      replaceClpMatchEquals(op0, f);
      return;
    }

    if (functionName.equals(SqlKind.FILTER.lowerName)) {
      isFilterExpression = true;
    }

    // Function isn't a CLP function that needs rewriting, but the arguments might be, so we recursively process them.
    for (Expression op : function.getOperands()) {
      tryRewritingExpression(op, isFilterExpression);
    }
  }

  /**
   * @param function
   * @return Whether the function call is `NOT clpMatch(...) = true`
   */
  private boolean isInvertedClpMatchEqualsFunctionCall(Function function) {
    // Validate this is a "NOT" function call
    if (!function.getOperator().equals(SqlKind.NOT.name())) {
      return false;
    }

    // Validate the first operand is a function call
    List<Expression> operands = function.getOperands();
    Expression op0 = operands.get(0);
    if (!op0.getType().equals(ExpressionType.FUNCTION)) {
      return false;
    }

    return isClpMatchEqualsFunctionCall(op0.getFunctionCall());
  }

  /**
   * @param function
   * @return Whether the function call is `clpMatch(...) = true`
   */
  private boolean isClpMatchEqualsFunctionCall(Function function) {
    // Validate this is an equals function call
    if (!function.getOperator().equals(SqlKind.EQUALS.name())) {
      return false;
    }

    // Validate operands are a function and a literal
    List<Expression> operands = function.getOperands();
    Expression op0 = operands.get(0);
    Expression op1 = operands.get(1);
    if (!op0.getType().equals(ExpressionType.FUNCTION) || !op1.getType().equals(ExpressionType.LITERAL)) {
      return false;
    }

    // Validate the left operand is clpMatch and the right is true
    Function f = op0.getFunctionCall();
    Literal l = op1.getLiteral();
    return f.getOperator().equals(_CLPMATCH_LOWERCASE_FUNCTION_NAME) && l.isSetBoolValue() && l.getBoolValue();
  }

  /**
   * Replaces `clpMatch(...) = true` with the boolean expression equivalent to `clpMatch(...)`
   * @param expression
   * @param function
   */
  private void replaceClpMatchEquals(Expression expression, Function function) {
    // Replace clpMatch with the equivalent boolean expression and then replace
    // `clpMatch(...) = true` with this boolean expression
    List<Expression> operands = function.getOperands();
    Expression op0 = operands.get(0);
    rewriteClpMatchFunction(op0);
    expression.setFunctionCall(op0.getFunctionCall());
  }

  /**
   * Rewrites `clpMatch(...)` using CLP's query translation logic
   * @param expression
   */
  private void rewriteClpMatchFunction(Expression expression) {
    Function currentFunction = expression.getFunctionCall();
    List<Expression> operands = currentFunction.getOperands();

    if (operands.size() == 2) {
      // Handle clpMatch("<columnGroupName>", '<query>')

      Expression op0 = operands.get(0);
      if (ExpressionType.IDENTIFIER != op0.getType()) {
        throw new SqlCompilationException("clpMatch: 1st operand must be an identifier.");
      }
      String columnGroupName = op0.getIdentifier().getName();

      Expression op1 = operands.get(1);
      if (ExpressionType.LITERAL != op1.getType()) {
        throw new SqlCompilationException("clpMatch: 2nd operand must be a literal.");
      }
      String wildcardQuery = op1.getLiteral().getStringValue();

      rewriteClpMatchFunction(expression, columnGroupName + LOGTYPE_COLUMN_SUFFIX,
          columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX, columnGroupName + ENCODED_VARS_COLUMN_SUFFIX, wildcardQuery);
    } else if (operands.size() == 4) {
      // Handle clpMatch("<columnGroupName>_logtype", "<columnGroupName>_dictionaryVars",
      //                 "<columnGroupName>_encodedVars", '<query>')

      for (int i = 0; i < 3; i++) {
        Expression op = operands.get(i);
        if (ExpressionType.IDENTIFIER != op.getType()) {
          throw new SqlCompilationException("clpMatch: First three operands must be an identifiers.");
        }
      }
      int i = 0;
      String logtypeColumnName = operands.get(i++).getIdentifier().getName();
      String dictVarsColumnName = operands.get(i++).getIdentifier().getName();
      String encodedVarsColumnName = operands.get(i++).getIdentifier().getName();

      Expression op3 = operands.get(i);
      if (ExpressionType.LITERAL != op3.getType()) {
        throw new SqlCompilationException("clpMatch: 4th operand must be a literal.");
      }
      String wildcardQuery = op3.getLiteral().getStringValue();

      rewriteClpMatchFunction(expression, logtypeColumnName, dictVarsColumnName, encodedVarsColumnName, wildcardQuery);
    } else {
      // Wrong number of args
      throw new SqlCompilationException("clpMatch: Too few/many operands - only 2 or 4 operands are expected.");
    }
  }

  /**
   * Rewrites `clpMatch(...)` using CLP's query translation logic
   * @param expression
   * @param logtypeColumnName
   * @param dictionaryVarsColumnName
   * @param encodedVarsColumnName
   * @param wildcardQuery
   */
  private void rewriteClpMatchFunction(Expression expression, String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, String wildcardQuery) {
    if (wildcardQuery.isEmpty()) {
      // Return `columnGroupName_logtype = ''`
      Function f = new Function(SqlKind.EQUALS.name());
      f.addToOperands(RequestUtils.getIdentifierExpression(logtypeColumnName));
      f.addToOperands(RequestUtils.getLiteralExpression(""));
      expression.setFunctionCall(f);
      return;
    }

    EightByteClpWildcardQueryEncoder queryEncoder =
        new EightByteClpWildcardQueryEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EightByteClpEncodedSubquery[] subqueries = queryEncoder.encode(wildcardQuery);

    Function subqueriesFunc;
    boolean requireDecompAndMatch = false;
    if (1 == subqueries.length) {
      ClpSqlSubqueryGenerationResult result =
          convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, wildcardQuery, 0,
              subqueries);
      requireDecompAndMatch = result.requiresDecompAndMatch();
      subqueriesFunc = result.getSqlFunc();
    } else {
      subqueriesFunc = new Function(SqlKind.OR.name());

      for (int i = 0; i < subqueries.length; i++) {
        ClpSqlSubqueryGenerationResult result =
            convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, wildcardQuery, i,
                subqueries);
        if (result.requiresDecompAndMatch()) {
          requireDecompAndMatch = true;
        }
        Function f = result.getSqlFunc();
        Expression e = new Expression(ExpressionType.FUNCTION);
        e.setFunctionCall(f);
        subqueriesFunc.addToOperands(e);
      }
    }

    Function newFunc;
    if (!requireDecompAndMatch) {
      newFunc = subqueriesFunc;
    } else {
      newFunc = new Function(SqlKind.AND.name());

      Expression e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(subqueriesFunc);
      newFunc.addToOperands(e);

      Function clpDecodeCall = new Function(_CLPDECODE_LOWERCASE_TRANSFORM_NAME);
      addCLPDecodeOperands(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, Literal.stringValue(""),
          clpDecodeCall);

      Function clpDecodeLike = new Function(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME);
      e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(clpDecodeCall);
      clpDecodeLike.addToOperands(e);

      e = new Expression(ExpressionType.LITERAL);
      e.setLiteral(Literal.stringValue(wildcardQueryToRegex(wildcardQuery)));
      clpDecodeLike.addToOperands(e);

      e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(clpDecodeLike);
      newFunc.addToOperands(e);
    }

    expression.setFunctionCall(newFunc);
  }

  /**
   * Rewrites the given instance of clpDecode as described in the class' Javadoc
   * @param expression clpDecode function expression
   */
  private void rewriteCLPDecodeFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    List<Expression> arguments = function.getOperands();

    // Validate clpDecode's arguments
    int numArgs = arguments.size();
    if (numArgs < 1 || numArgs > 2) {
      // Too few/many args for this rewriter, so do nothing and let it pass through to the clpDecode transform function
      return;
    }

    Expression arg0 = arguments.get(0);
    if (ExpressionType.IDENTIFIER != arg0.getType()) {
      throw new SqlCompilationException("clpDecode: 1st argument must be a column group name (identifier).");
    }
    String columnGroupName = arg0.getIdentifier().getName();

    Literal defaultValueLiteral = null;
    if (numArgs > 1) {
      Expression arg1 = arguments.get(1);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpDecode: 2nd argument must be a default value (literal).");
      }
      defaultValueLiteral = arg1.getLiteral();
    }

    // Replace the columnGroup with the individual columns
    arguments.clear();
    addCLPDecodeOperands(columnGroupName, defaultValueLiteral, function);
  }

  private ClpSqlSubqueryGenerationResult convertSubqueryToSql(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, String wildcardQuery, int subqueryIdx, EightByteClpEncodedSubquery[] subqueries) {
    EightByteClpEncodedSubquery subquery = subqueries[subqueryIdx];

    if (!subquery.containsVariables()) {
      Function f = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
          subquery.logtypeQueryContainsWildcards());
      return new ClpSqlSubqueryGenerationResult(false, f);
    }

    Function subqueryFunc = new Function(SqlKind.AND.name());

    Expression e;

    // Add logtype query
    Function f = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
        subquery.logtypeQueryContainsWildcards());
    e = new Expression(ExpressionType.FUNCTION);
    e.setFunctionCall(f);
    subqueryFunc.addToOperands(e);

    // Add any dictionary variables
    int numDictVars = 0;
    for (ByteSegment dictVar : subquery.getDictVars()) {
      f = createStringColumnMatchFunction(SqlKind.EQUALS.name(), dictionaryVarsColumnName, dictVar.toString());
      e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(f);
      subqueryFunc.addToOperands(e);

      ++numDictVars;
    }

    // Add any encoded variables
    int numEncodedVars = 0;
    for (long encodedVar : subquery.getEncodedVars()) {
      f = new Function(SqlKind.EQUALS.name());
      f.addToOperands(RequestUtils.getIdentifierExpression(encodedVarsColumnName));
      f.addToOperands(RequestUtils.getLiteralExpression(encodedVar));

      e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(f);
      subqueryFunc.addToOperands(e);

      ++numEncodedVars;
    }

    // Add any wildcard dictionary variables
    for (VariableWildcardQuery varWildcardQuery : subquery.getDictVarWildcardQueries()) {
      f = createStringColumnMatchFunction(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME, dictionaryVarsColumnName,
          wildcardQueryToRegex(varWildcardQuery.getQuery().toString()));
      e = new Expression(ExpressionType.FUNCTION);
      e.setFunctionCall(f);
      subqueryFunc.addToOperands(e);

      ++numDictVars;
    }

    // Add any wildcard encoded variables
    int numEncodedVarWildcardQueries = subquery.getNumEncodedVarWildcardQueries();
    numEncodedVars += numEncodedVarWildcardQueries;
    if (numEncodedVarWildcardQueries > 0) {
      // Create call to clpEncodedVarsMatch
      Expression clpEncodedVarsExp = RequestUtils.getFunctionExpression(
          RequestUtils.canonicalizeFunctionNamePreservingSpecialKey(
              TransformFunctionType.CLP_ENCODED_VARS_MATCH.getName()));
      f = clpEncodedVarsExp.getFunctionCall();
      f.addToOperands(RequestUtils.getIdentifierExpression(logtypeColumnName));
      f.addToOperands(RequestUtils.getIdentifierExpression(encodedVarsColumnName));
      f.addToOperands(RequestUtils.getLiteralExpression(wildcardQuery));
      f.addToOperands(RequestUtils.getLiteralExpression(subqueryIdx));

      // Create `clpEncodedVarsMatch(...) = true`
      e = RequestUtils.getFunctionExpression(SqlKind.EQUALS.name());
      f = e.getFunctionCall();
      f.addToOperands(clpEncodedVarsExp);
      f.addToOperands(RequestUtils.getLiteralExpression(true));

      subqueryFunc.addToOperands(e);
    }

    // We require a decompress and match in the following cases:
    // 1. There are >1 variables of a specific type (dict/encoded) in the query. Consider this query: " dv123 dv456 ".
    //    The corresponding SQL will look like:
    //    "x_logtype = '...' AND x_dictionaryVars = 'dv123' AND x_dictionaryVars = 'dv456'"
    //    This SQL will indeed match values which also match the query; but this SQL will also match values which
    //    *don't* match the query, like " dv456 dv123 ". This is because the SQL query doesn't encode the position of
    //    the variables.
    // 2. There is no more than 1 variable of each type, but the logtype query contains wildcards. Consider this query:
    //    "user dv123 *". The corresponding SQL will look like:
    //    "REGEXP_LIKE(x_logtype, "user: \dv .*") AND x_dictionaryVars = 'dv123'",
    //    where "\dv" is a dictionary variable placeholder. This SQL could match the
    //    value "user dv123 joined" but it could also match "user dv456 joined dv123".
    boolean requiresDecompAndMatch =
        !(numDictVars < 2 && numEncodedVars < 2 && !subquery.logtypeQueryContainsWildcards());
    return new ClpSqlSubqueryGenerationResult(requiresDecompAndMatch, subqueryFunc);
  }

  private Function createLogtypeMatchFunction(String columnName, String query, boolean containsWildcards) {
    String funcName;
    String funcQuery;
    if (containsWildcards) {
      funcName = _REGEXP_LIKE_LOWERCASE_FUNCTION_NAME;
      funcQuery = wildcardQueryToRegex(query);
    } else {
      funcName = SqlKind.EQUALS.name();
      funcQuery = query;
    }
    return createStringColumnMatchFunction(funcName, columnName, funcQuery);
  }

  private Function createStringColumnMatchFunction(String canonicalName, String columnName, String query) {
    Function func = new Function(canonicalName);
    func.addToOperands(RequestUtils.getIdentifierExpression(columnName));
    func.addToOperands(RequestUtils.getLiteralExpression(query));
    return func;
  }

  /**
   * Converts a CLP-encoded column group into physical column names and adds them to the CLPDecode transform function's
   * operands.
   * @param columnGroupName Name of the CLP-encoded column group
   * @param defaultValueLiteral Optional default value to pass through to the transform function
   * @param clpDecode The function to add the operands to
   */
  private void addCLPDecodeOperands(String columnGroupName, @Nullable Literal defaultValueLiteral, Function clpDecode) {
    addCLPDecodeOperands(columnGroupName + LOGTYPE_COLUMN_SUFFIX, columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX,
        columnGroupName + ENCODED_VARS_COLUMN_SUFFIX, defaultValueLiteral, clpDecode);
  }

  /**
   * Adds the given operands to the given CLPDecode transform function.
   * @param logtypeColumnName
   * @param dictionaryVarsColumnName
   * @param encodedVarsColumnName
   * @param defaultValueLiteral
   * @param clpDecode
   */
  private void addCLPDecodeOperands(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, @Nullable Literal defaultValueLiteral, Function clpDecode) {
    clpDecode.addToOperands(RequestUtils.getIdentifierExpression(logtypeColumnName));
    clpDecode.addToOperands(RequestUtils.getIdentifierExpression(dictionaryVarsColumnName));
    clpDecode.addToOperands(RequestUtils.getIdentifierExpression(encodedVarsColumnName));
    if (null != defaultValueLiteral) {
      Expression e = new Expression(ExpressionType.LITERAL);
      e.setLiteral(defaultValueLiteral);
      clpDecode.addToOperands(e);
    }
  }

  /**
   * Converts a wildcard query into a regular expression. The wildcard query is a string which may contain two possible
   * wildcards:
   * 1. '*' that matches zero or more characters.
   * 2. '?' that matches any single character.
   * @param wildcardQuery
   * @return The regular expression which matches the same values as the wildcard query.
   */
  private static String wildcardQueryToRegex(String wildcardQuery) {
    boolean isEscaped = false;
    StringBuilder queryWithSqlWildcards = new StringBuilder();

    // Add begin anchor if necessary
    if (!wildcardQuery.isEmpty() && '*' != wildcardQuery.charAt(0)) {
      queryWithSqlWildcards.append('^');
    }

    int uncopiedIdx = 0;
    for (int queryIdx = 0; queryIdx < wildcardQuery.length(); queryIdx++) {
      char queryChar = wildcardQuery.charAt(queryIdx);
      if (isEscaped) {
        isEscaped = false;
      } else {
        if ('\\' == queryChar) {
          isEscaped = true;
        } else if (isWildcard(queryChar)) {
          queryWithSqlWildcards.append(wildcardQuery, uncopiedIdx, queryIdx);
          queryWithSqlWildcards.append('.');
          uncopiedIdx = queryIdx;
        } else {
          for (final char metaChar : _NON_WILDCARD_REGEX_META_CHARACTERS) {
            if (metaChar == queryChar) {
              queryWithSqlWildcards.append(wildcardQuery, uncopiedIdx, queryIdx);
              queryWithSqlWildcards.append('\\');
              uncopiedIdx = queryIdx;
              break;
            }
          }
        }
      }
    }
    if (uncopiedIdx < wildcardQuery.length()) {
      queryWithSqlWildcards.append(wildcardQuery, uncopiedIdx, wildcardQuery.length());
    }

    // Add end anchor if necessary
    if (!wildcardQuery.isEmpty() && '*' != wildcardQuery.charAt(wildcardQuery.length() - 1)) {
      queryWithSqlWildcards.append('$');
    }

    return queryWithSqlWildcards.toString();
  }

  /**
   * @param c
   * @return Whether the given character is a wildcard.
   */
  private static boolean isWildcard(char c) {
    return '*' == c || '?' == c;
  }

  /**
   * Simple class to hold the result of turning a CLP subquery into SQL.
   */
  private static class ClpSqlSubqueryGenerationResult {
    private final boolean _requiresDecompAndMatch;
    private final Function _sqlFunc;

    ClpSqlSubqueryGenerationResult(boolean requiresDecompAndMatch, Function sqlFunc) {
      _requiresDecompAndMatch = requiresDecompAndMatch;
      _sqlFunc = sqlFunc;
    }

    public boolean requiresDecompAndMatch() {
      return _requiresDecompAndMatch;
    }

    public Function getSqlFunc() {
      return _sqlFunc;
    }
  }
}
