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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Query rewriter to rewrite clpDecode so that users can pass in the name of a CLP-encoded column group instead of the
 * names of all the columns in the group.
 * <p>
 * Usage:
 * <pre>
 *   clpDecode("columnGroupName"[, defaultValue])
 * </pre>
 * which will be rewritten to:
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
 */
public class CLPDecodeRewriter implements QueryRewriter {
  public static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  public static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  public static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  private static final String _CLPDECODE_LOWERCASE_TRANSFORM_NAME =
      TransformFunctionType.CLPDECODE.getName().toLowerCase();

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectExpressions = pinotQuery.getSelectList();
    if (null != selectExpressions) {
      for (Expression e : selectExpressions) {
        tryRewritingExpression(e);
      }
    }
    List<Expression> groupByExpressions = pinotQuery.getGroupByList();
    if (null != groupByExpressions) {
      for (Expression e : groupByExpressions) {
        tryRewritingExpression(e);
      }
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (null != orderByExpressions) {
      for (Expression e : orderByExpressions) {
        tryRewritingExpression(e);
      }
    }
    tryRewritingExpression(pinotQuery.getFilterExpression());
    tryRewritingExpression(pinotQuery.getHavingExpression());
    return pinotQuery;
  }

  /**
   * Rewrites any instances of clpDecode in the given expression
   * @param expression Expression which may contain instances of clpDecode
   */
  private void tryRewritingExpression(Expression expression) {
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

    // Function isn't a CLP function that needs rewriting, but the arguments might be, so we recursively process them.
    for (Expression op : function.getOperands()) {
      tryRewritingExpression(op);
    }
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

  /**
   * Adds the CLPDecode transform function's operands to the given function
   * @param columnGroupName Name of the CLP-encoded column group
   * @param defaultValueLiteral Optional default value to pass through to the transform function
   * @param clpDecode The function to add the operands to
   */
  private void addCLPDecodeOperands(String columnGroupName, @Nullable Literal defaultValueLiteral, Function clpDecode) {
    Expression e;

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + LOGTYPE_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + ENCODED_VARS_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);

    if (null != defaultValueLiteral) {
      e = new Expression(ExpressionType.LITERAL);
      e.setLiteral(defaultValueLiteral);
      clpDecode.addToOperands(e);
    }
  }
}
