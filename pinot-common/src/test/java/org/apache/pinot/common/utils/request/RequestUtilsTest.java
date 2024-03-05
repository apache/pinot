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
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestUtilsTest {
  // please check comments inside RequestUtils.getLiteralExpression() for why we need this test
  @Test
  public void testGetLiteralExpressionForObject() {
    Expression literalExpression = RequestUtils.getLiteralExpression(Float.valueOf(0.06f));
    Assert.assertEquals((literalExpression.getLiteral().getDoubleValue()), 0.06);
  }

  @Test
  public void testGetLiteralExpressionForPrimitiveLong() {
    Expression literalExpression = RequestUtils.getLiteralExpression(4500L);
    Assert.assertTrue(literalExpression.getLiteral().isSetLongValue());
    Assert.assertFalse(literalExpression.getLiteral().isSetDoubleValue());
    Assert.assertEquals(literalExpression.getLiteral().getLongValue(), 4500L);
  }

  @Test
  public void testParseQuery() {
    SqlNodeAndOptions result = RequestUtils.parseQuery("select foo from countries where bar > 1");
    Assert.assertTrue(result.getParseTimeNs() > 0);
    Assert.assertEquals(result.getSqlType(), PinotSqlType.DQL);
    Assert.assertEquals(result.getSqlNode().toSqlString((SqlDialect) null).toString(),
        "SELECT `foo`\n" + "FROM `countries`\n" + "WHERE `bar` > 1");
  }
}
