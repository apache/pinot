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
package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BetweenFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("intSVColumn", FieldSpec.DataType.INT)
      .addMultiValueDimension("intMVColumn", FieldSpec.DataType.INT).build();

  @Test
  public void testBetweenRewriteOnSVColumn() {
    Range expectedRange = new Range(5, false, 10, true);
    // intSVColumn BETWEEN 5.5 AND 10.5 should be rewritten to intSVColumn > 5 AND intSVColumn <= 10 due to a
    // combination of the BetweenFilterOptimizer and the NumericalFilterOptimizer
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intSVColumn BETWEEN 5.5 AND 10.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:RANGE, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intSVColumn)), Expression(type:LITERAL, literal:<Literal stringValue:"
            + expectedRange.getRangeString() + ">)]))");
  }

  @Test
  public void testBetweenNoRewriteOnMVColumn() {
    // intMVColumn BETWEEN 5.5 AND 10.5 should not be rewritten
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intMVColumn BETWEEN 5.5 AND 10.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:BETWEEN, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intMVColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5"
            + ".5>), Expression(type:LITERAL, literal:<Literal doubleValue:10.5>)]))");
  }

  private static String rewrite(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    return pinotQuery.getFilterExpression().toString();
  }
}
