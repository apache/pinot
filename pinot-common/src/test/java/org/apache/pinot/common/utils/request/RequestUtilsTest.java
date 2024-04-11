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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RequestUtilsTest {

  @Test
  public void testNullLiteralParsing() {
    SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    Expression nullExpr = RequestUtils.getLiteralExpression(nullLiteral);
    assertEquals(nullExpr.getType(), ExpressionType.LITERAL);
    assertTrue(nullExpr.getLiteral().getNullValue());
  }

  // please check comments inside RequestUtils.getLiteralExpression() for why we need this test
  @Test
  public void testGetLiteralExpressionForObject() {
    Expression literalExpression = RequestUtils.getLiteralExpression(Float.valueOf(0.06f));
    assertEquals((literalExpression.getLiteral().getDoubleValue()), 0.06);
  }

  @Test
  public void testGetLiteralExpressionForPrimitiveLong() {
    Expression literalExpression = RequestUtils.getLiteralExpression(4500L);
    assertTrue(literalExpression.getLiteral().isSetLongValue());
    assertFalse(literalExpression.getLiteral().isSetDoubleValue());
    assertEquals(literalExpression.getLiteral().getLongValue(), 4500L);
  }

  @Test
  public void testParseQuery() {
    SqlNodeAndOptions result = RequestUtils.parseQuery("select foo from countries where bar > 1");
    assertTrue(result.getParseTimeNs() > 0);
    assertEquals(result.getSqlType(), PinotSqlType.DQL);
    assertEquals(result.getSqlNode().toSqlString((SqlDialect) null).toString(),
        "SELECT `foo`\n" + "FROM `countries`\n" + "WHERE `bar` > 1");
  }
}
