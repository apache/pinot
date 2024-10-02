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

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


public class ClpRewriterTest {
  private static final QueryRewriter _QUERY_REWRITER = new ClpRewriter();

  @Test
  public void testCLPDecodeRewrite() {
    // clpDecode rewrite from column group to individual columns
    testQueryRewrite("SELECT clpDecode(message) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite("SELECT clpDecode(message, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");

    // clpDecode passthrough
    testQueryRewrite("SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite(
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");
  }

  @Test
  public void testClpMatchRewrite() {
    MessageEncoder encoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EncodedMessage encodedMessage = new EncodedMessage();
    try {
      String message;
      String[] dictionaryVars;
      Long[] encodedVars;

      // Query with no wildcards and no variables
      message = " INFO container ";
      encoder.encodeMessage(message, encodedMessage);
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s'", encodedMessage.getLogTypeAsString())
      );

      // Query with no wildcards and no variables using individual column names
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message_logtype, message_dictionaryVars,"
              + " message_encodedVars, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s'", encodedMessage.getLogTypeAsString())
      );

      // Query with wildcards and no variables
      message = " INFO container ";
      encoder.encodeMessage(message, encodedMessage);
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '*%s*')", message),
          String.format("SELECT * FROM clpTable WHERE REGEXP_LIKE(message_logtype, '.*%s.*')",
              encodedMessage.getLogTypeAsString())
      );

      // Query with no wildcards and a single dictionary var
      message = " var123 ";
      encoder.encodeMessage(message, encodedMessage);
      dictionaryVars = encodedMessage.getDictionaryVarsAsStrings();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s' AND message_dictionaryVars = '%s'",
              encodedMessage.getLogTypeAsString(), dictionaryVars[0])
      );

      // Query with no wildcards and a single encoded var
      message = " 123 ";
      encoder.encodeMessage(message, encodedMessage);
      encodedVars = encodedMessage.getEncodedVarsAsBoxedLongs();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s' AND message_encodedVars = %s",
              encodedMessage.getLogTypeAsString(), encodedVars[0])
      );

      // Query with no wildcards and multiple dictionary vars
      message = " var123 var456 ";
      encoder.encodeMessage(message, encodedMessage);
      dictionaryVars = encodedMessage.getDictionaryVarsAsStrings();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s' AND message_dictionaryVars = '%s'"
                  + " AND message_dictionaryVars = '%s'"
                  + " AND REGEXP_LIKE(clpdecode(message_logtype, message_dictionaryVars, message_encodedVars, ''),"
                  + " '%s')",
              encodedMessage.getLogTypeAsString(), dictionaryVars[0], dictionaryVars[1], String.format("^%s$", message))
      );

      // Query with no wildcards and multiple encoded vars
      message = " 123 456 ";
      encoder.encodeMessage(message, encodedMessage);
      encodedVars = encodedMessage.getEncodedVarsAsBoxedLongs();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s' AND message_encodedVars = %s"
                  + " AND message_encodedVars = %s"
                  + " AND REGEXP_LIKE(clpdecode(message_logtype, message_dictionaryVars, message_encodedVars, ''),"
                  + " '%s')",
              encodedMessage.getLogTypeAsString(), encodedVars[0], encodedVars[1], String.format("^%s$", message))
      );

      // Query with no wildcards, a dictionary var, and an encoded var
      message = " var123 456 ";
      encoder.encodeMessage(message, encodedMessage);
      dictionaryVars = encodedMessage.getDictionaryVarsAsStrings();
      encodedVars = encodedMessage.getEncodedVarsAsBoxedLongs();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '%s')", message),
          String.format("SELECT * FROM clpTable WHERE message_logtype = '%s' AND message_dictionaryVars = '%s'"
                  + " AND message_encodedVars = %s", encodedMessage.getLogTypeAsString(), dictionaryVars[0],
              encodedVars[0])
      );

      // Query with wildcards for a single dictionary var
      message = "var123";
      encoder.encodeMessage(message, encodedMessage);
      dictionaryVars = encodedMessage.getDictionaryVarsAsStrings();
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '*%s*')", message),
          String.format("SELECT * FROM clpTable WHERE REGEXP_LIKE(message_logtype, '.*%s.*')"
                  + " AND REGEXP_LIKE(message_dictionaryVars, '.*%s.*')"
                  + " AND REGEXP_LIKE(clpdecode(message_logtype, message_dictionaryVars, message_encodedVars, ''),"
                  + " '%s')",
              encodedMessage.getLogTypeAsString(), dictionaryVars[0], String.format(".*%s.*", message))
      );

      // Query with wildcards for a single var which could be a float encoded var, int encoded var, or dictionary var
      encoder.encodeMessage("123", encodedMessage);
      String subquery1Logtype = encodedMessage.getLogTypeAsString();
      encoder.encodeMessage("123.0", encodedMessage);
      String subquery2Logtype = encodedMessage.getLogTypeAsString();
      encoder.encodeMessage("var123", encodedMessage);
      String subquery3Logtype = encodedMessage.getLogTypeAsString();
      message = "123";
      testQueryRewrite(
          String.format("SELECT * FROM clpTable WHERE clpMatch(message, '*%s*')", message),
          String.format("SELECT * FROM clpTable WHERE ("
                  + "(REGEXP_LIKE(message_logtype, '.*%s.*')"
                  + " AND clpEncodedVarsMatch(message_logtype, message_encodedVars, '*%s*', 0))"
                  + " OR (REGEXP_LIKE(message_logtype, '.*%s.*')"
                  + " AND clpEncodedVarsMatch(message_logtype, message_encodedVars, '*%s*', 1))"
                  + " OR (REGEXP_LIKE(message_logtype, '.*%s.*') AND REGEXP_LIKE(message_dictionaryVars, '.*%s.*'))"
                  + ") AND REGEXP_LIKE(clpdecode(message_logtype, message_dictionaryVars, message_encodedVars, ''),"
                  + " '.*%s.*')",
          subquery1Logtype, message, subquery2Logtype, message, subquery3Logtype, message, message)
      );
    } catch (IOException e) {
      fail("Failed to encode message", e);
    }
  }

  /**
   * Flattens an AND expression such that any of its children that are AND ops are elided by adding their operands to
   * the given expression.
   * <p>
   * Ex: "x = '1' AND ('y' = 2 AND NOT 'z' = 3)" would be flattened to
   * "x '1' AND 'y' = 2 AND NOT 'z' = 3"
   * @param expr
   */
  private void flattenAndExpression(Expression expr) {
    List<Expression> newOperands = new ArrayList<>();
    Function func = expr.getFunctionCall();
    for (Expression childOp : func.getOperands()) {
      if (!childOp.isSetFunctionCall()) {
        newOperands.add(childOp);
        continue;
      }

      Function childFunc = childOp.getFunctionCall();
      if (childFunc.getOperator().equals(SqlKind.AND.name())) {
        flattenAndExpression(childOp);
        newOperands.addAll(childOp.getFunctionCall().getOperands());
      } else {
        flattenAllAndExpressions(childOp);
        newOperands.add(childOp);
      }
    }
    func.setOperands(newOperands);
  }

  /**
   * Flattens all AND expressions in a given expression.
   * <p>
   * Ex: "a = 0 OR (x = '1' AND ('y' = 2 AND NOT 'z' = 3))" would be flattened to
   * "a = 0 OR (x '1' AND 'y' = 2 AND NOT 'z' = 3)"
   * @param expr
   */
  private void flattenAllAndExpressions(Expression expr) {
    if (!expr.isSetFunctionCall()) {
      return;
    }

    List<Expression> newOperands = new ArrayList<>();

    Function func = expr.getFunctionCall();
    if (func.getOperator().equals(SqlKind.AND.name())) {
      flattenAndExpression(expr);
      return;
    }

    // Recursively handle the expression's operands
    for (Expression childOp : func.getOperands()) {
      if (!childOp.isSetFunctionCall()) {
        newOperands.add(childOp);
        continue;
      }

      Function childFunc = childOp.getFunctionCall();
      if (childFunc.getOperator().equals(SqlKind.AND.name())) {
        flattenAndExpression(childOp);
      } else {
        flattenAllAndExpressions(childOp);
      }
      newOperands.add(childOp);
    }

    func.setOperands(newOperands);
  }

  @Test
  public void testUnsupportedCLPDecodeQueries() {
    testUnsupportedQuery("SELECT clpDecode('message') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', 'default') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', default) FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode(message, default) FROM clpTable");
  }

  private void testQueryRewrite(String original, String expected) {
    PinotQuery originalQuery = _QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original));
    PinotQuery expectedQuery = CalciteSqlParser.compileToPinotQuery(expected);
    // Flatten any AND expressions in the rewritten query.
    // NOTE: The rewritten query may have nested AND conditions of the form (A AND (B AND C)). If we don't flatten them,
    // comparison with the expected query will fail.
    Expression origQueryFilterExpr = originalQuery.getFilterExpression();
    if (null != origQueryFilterExpr) {
      flattenAllAndExpressions(origQueryFilterExpr);
    }
    assertEquals(originalQuery, expectedQuery);
  }

  private void testUnsupportedQuery(String query) {
    assertThrows(SqlCompilationException.class,
        () -> _QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query)));
  }
}
