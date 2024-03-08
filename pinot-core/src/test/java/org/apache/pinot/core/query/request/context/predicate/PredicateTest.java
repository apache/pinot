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
package org.apache.pinot.core.query.request.context.predicate;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PredicateTest {

  @Test
  public void testSerDe() {
    // EqPredicate
    assertEquals(testSerDe("foo\t= 123"), "foo = '123'");
    assertEquals(testSerDe("foo='123'"), "foo = '123'");

    // NotEqPredicate
    assertEquals(testSerDe("foo !=123"), "foo != '123'");
    assertEquals(testSerDe("foo <>\t123"), "foo != '123'");
    assertEquals(testSerDe("foo\t\t!= '123'"), "foo != '123'");
    assertEquals(testSerDe("foo<> '123'"), "foo != '123'");

    // InPredicate
    assertEquals(testSerDe("foo in (123, \t456,789)"), "foo IN ('123','456','789')");
    assertEquals(testSerDe("foo In (\t '123', '456',  '789')"), "foo IN ('123','456','789')");

    // NotInPredicate
    assertEquals(testSerDe("foo NOT in (123, \t456,789)"), "foo NOT IN ('123','456','789')");
    assertEquals(testSerDe("foo NoT In (\t '123', '456',  '789')"), "foo NOT IN ('123','456','789')");

    // RangePredicate
    assertEquals(testSerDe("foo >123"), "foo > '123'");
    assertEquals(testSerDe("foo >=\t'123'"), "foo >= '123'");
    assertEquals(testSerDe("foo< 123"), "foo < '123'");
    assertEquals(testSerDe("foo\t<= '123'"), "foo <= '123'");
    assertEquals(testSerDe("foo bEtWeEn 123 AnD 456"), "foo BETWEEN '123' AND '456'");

    // RegexpLikePredicate
    assertEquals(testSerDe("ReGexP_lIKe(foo,\t\t'bar')"), "regexp_like(foo,'bar')");

    // TextMatchPredicate
    assertEquals(testSerDe("TEXT_MATCH(foo\t ,\t'bar')"), "text_match(foo,'bar')");

    // IsNullPredicate
    assertEquals(testSerDe("foo\tis\tnull"), "foo IS NULL");

    // IsNotNullPredicate
    assertEquals(testSerDe("foo\tIS not\tNulL"), "foo IS NOT NULL");

    // Non-standard RangePredicate (merged ranges)
    RangePredicate rangePredicate =
        new RangePredicate(ExpressionContext.forIdentifier("foo"), true, "123", false, "456",
            FieldSpec.DataType.STRING);
    String predicateExpression = rangePredicate.toString();
    assertEquals(predicateExpression, "(foo >= '123' AND foo < '456')");
    Expression thriftExpression = CalciteSqlParser.compileToExpression(predicateExpression);
    FilterContext filter = RequestContextUtils.getFilter(thriftExpression);
    assertEquals(filter.getType(), FilterContext.Type.AND);
    List<FilterContext> children = filter.getChildren();
    assertEquals(children.size(), 2);
    assertEquals(children.get(0).toString(), "foo >= '123'");
    assertEquals(children.get(1).toString(), "foo < '456'");
  }

  /**
   * Tests that the serialized predicate can be parsed and converted back to the same predicate, and returns the
   * serialized predicate (standardized string representation of the predicate expression).
   */
  private String testSerDe(String predicateExpression) {
    // Parse and convert the string predicate expression into Predicate
    Expression thriftExpression = CalciteSqlParser.compileToExpression(predicateExpression);
    FilterContext filter = RequestContextUtils.getFilter(thriftExpression);
    assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    Predicate predicate = filter.getPredicate();

    // Serialize the predicate into string
    predicateExpression = predicate.toString();

    // Deserialize and compare
    thriftExpression = CalciteSqlParser.compileToExpression(predicateExpression);
    filter = RequestContextUtils.getFilter(thriftExpression);
    assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    assertEquals(filter.getPredicate(), predicate);

    return predicateExpression;
  }
}
