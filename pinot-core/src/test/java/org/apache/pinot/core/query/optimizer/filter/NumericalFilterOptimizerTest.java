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


public class NumericalFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn", FieldSpec.DataType.INT)
          .addSingleValueDimension("longColumn", FieldSpec.DataType.LONG)
          .addSingleValueDimension("floatColumn", FieldSpec.DataType.FLOAT)
          .addSingleValueDimension("doubleColumn", FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension("stringColumn", FieldSpec.DataType.STRING)
          .addSingleValueDimension("bytesColumn", FieldSpec.DataType.BYTES)
          .addMultiValueDimension("mvIntColumn", FieldSpec.DataType.INT).build();

  @Test
  public void testEqualsRewrites() {
    // Test int column equals valid decimal value. "intColumn = 5.0" should have been rewritten to "intColumn = 5"
    // since 5.0 converts to integer value without any loss of information.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");

    // Test a query containing NOT operator
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE NOT intColumn = 5.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT, operands:[Expression(type:FUNCTION, "
            + "functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier"
            + "(name:intColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))]))");

    // Test int column equals invalid decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5.5"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test int column not equals valid decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");

    // Test int column not equals invalid decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5.5"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test int column equals out of domain value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test int column not equals out of domain value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test long column equals valid decimal value. "longColumn = 5.0" should be rewritten to "longColumn = 5" since 5.0
    // converts to long value without any loss of information.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn = 5.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");

    // Test long column equals invalid decimal value. "longColumn = 5.5" should have been rewritten to "false" literal
    // since an LONG column can never have a decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn = 5.5"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test long column with valid decimal value. "longColumn = 5.5" should have been rewritten to "false" literal since
    // LONG column can never have a decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn != 5.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");

    // Test long column not equals invalid decimal value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn != 5.5"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test float column equals valid long value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn = 5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)"
            + "]))");

    // Test float column equals invalid long value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn = " + String.valueOf(Long.MAX_VALUE)),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test float column not equals valid long value
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn != " + String.valueOf(Long.MAX_VALUE)),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test float column not equals valid long value
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn != 5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)"
            + "]))");

    // test double column equals valid long value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn = 5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)"
            + "]))");

    // Test double column equals invalid long value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn = " + String.valueOf(Long.MAX_VALUE)),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test double column not equals valid long value
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn != " + String.valueOf(Long.MAX_VALUE)),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test double column not equals invalid long value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn != 5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)"
            + "]))");
  }

  @Test
  public void testRangeRewrites() {

    // Test INT column with DOUBLE value greater than Integer.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn > 3000000000.0"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test INT column with DOUBLE value greater than Integer.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < 3000000000.0"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test INT column with DOUBLE value greater than Integer.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn > -3000000000.0"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test INT column with LONG value less than Integer.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < -3000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test INT column with LONG value greater than Integer.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn > 3000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test INT column with LONG value greater than Integer.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < 3000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test INT column with LONG value greater than Integer.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn > -3000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test INT column with LONG value less than Integer.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < -3000000000"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test INT column with DOUBLE value that falls within INT bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < -2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:-2100000000>)]))");

    // Test INT column with DOUBLE value that falls within INT bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn < 2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:2100000000>)]))");

    // Test FLOAT column greater than Double.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn > " + Double.MAX_VALUE),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test FLOAT column less than Double.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn < " + Double.MAX_VALUE),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test FLOAT column greater than -Double.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn > " + -Double.MAX_VALUE),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test FLOAT column less than -Double.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn < " + -Double.MAX_VALUE),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test FLOAT column greater than Long.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn > " + Long.MAX_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal "
            + "doubleValue:9.223372036854776E18>)]))");

    // Test FLOAT column less than Long.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn < " + Long.MIN_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal "
            + "doubleValue:-9.223372036854776E18>)]))");

    // Test FLOAT column greater than Long.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn > " + Long.MIN_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN, operands:[Expression(type:IDENTIFIER,"
            + " identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:-9"
            + ".223372036854776E18>)]))");

    // Test FLOAT column with DOUBLE value: no rewrite.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn > -2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal "
            + "doubleValue:-2.1000000005E9>)]))");

    // Test FLOAT column with DOUBLE value: no rewrite.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn < -2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:"
            + "-2.1000000005E9>)]))");

    // Test FLOAT column with DOUBLE value: no rewrite.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn <= 2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal "
            + "doubleValue:2.1000000005E9>)]))");

    // Test FLOAT column with DOUBLE value: no rewrite.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE floatColumn >= 2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN_OR_EQUAL, operands:[Expression("
            + "type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal "
            + "doubleValue:2.1000000005E9>)]))");

    // Test LONG column with DOUBLE value greater than Long.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn > 999999999999999999999999999999.9999"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test LONG column with DOUBLE value greater than Long.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn < 999999999999999999999999999999.9999"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test LONG column with DOUBLE value greater than Long.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn > -999999999999999999999999999999.9999"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test LONG column with DOUBLE value that falls within LONG bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn < -2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:-2100000000>)]))");

    // Test LONG column with DOUBLE value that falls within LONG bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn > -2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:-2100000000>)]))");

    // Test LONG column with DOUBLE value that falls within LONG bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn < 2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:2100000000>)]))");

    // Test LONG column with DOUBLE value that falls within LONG bounds.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE longColumn > 2100000000.5"),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN, operands:[Expression(type:IDENTIFIER,"
            + " identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal "
            + "longValue:2100000000>)]))");

    // Test DOUBLE column greater than Long.MAX_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn > " + Long.MAX_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal"
            + " doubleValue:9.223372036854776E18>)]))");

    // Test DOUBLE column less than Long.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn < " + Long.MIN_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:LESS_THAN_OR_EQUAL, operands:[Expression"
            + "(type:IDENTIFIER, identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal"
            + " doubleValue:-9.223372036854776E18>)]))");

    // Test DOUBLE column greater than Long.MIN_VALUE.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE doubleColumn > " + Long.MIN_VALUE),
        "Expression(type:FUNCTION, functionCall:Function(operator:GREATER_THAN, operands:[Expression(type:IDENTIFIER,"
            + " identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:-9"
            + ".223372036854776E18>)]))");
  }

  @Test
  public void testNull() {
    // Test column IS NOT NULL.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn IS NOT NULL"),
        "Expression(type:FUNCTION, functionCall:Function(operator:IS_NOT_NULL, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn))]))");

    // Test column IS NULL.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn IS NULL"),
        "Expression(type:FUNCTION, functionCall:Function(operator:IS_NULL, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn))]))");
  }

  @Test
  public void testAndRewrites() {
    // Test and with all true operands
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5.5 AND longColumn != 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test and with all false operands
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5.5 AND longColumn = 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test and with one false operand
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5.5 AND longColumn != 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test or with all true operands
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5.5 OR longColumn != 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    // Test int column with exclusive valid double value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn > 5.0 AND intColumn < 10.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:RANGE, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal stringValue:"
            + "(5\u000010)>)]))");

    // Test int column with inclusive valid double value.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn >= 5.0 AND intColumn <= 10.0"),
        "Expression(type:FUNCTION, functionCall:Function(operator:RANGE, operands:[Expression(type:IDENTIFIER, "
            + "identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal "
            + "stringValue:[5\u000010]>)]))");
  }

  @Test
  public void testOrRewrites() {
    // Test or with all false operands
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn = 5.5 OR longColumn = 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");

    // Test or with one false operand.
    Assert.assertEquals(rewrite("SELECT * FROM testTable WHERE intColumn != 5.5 OR longColumn = 6.4"),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  private static String rewrite(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    return pinotQuery.getFilterExpression().toString();
  }
}
