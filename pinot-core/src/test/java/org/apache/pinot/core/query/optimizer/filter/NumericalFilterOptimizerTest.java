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

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NumericalFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn", FieldSpec.DataType.INT)
          .addSingleValueDimension("longColumn", FieldSpec.DataType.LONG)
          .addSingleValueDimension("floatColumn", FieldSpec.DataType.FLOAT)
          .addSingleValueDimension("doubleColumn", FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension("stringColumn", FieldSpec.DataType.STRING)
          .addSingleValueDimension("bytesColumn", FieldSpec.DataType.BYTES)
          .addMultiValueDimension("mvIntColumn", FieldSpec.DataType.INT).build();

  @Test
  public void testEqualsIntColumnWithValidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5.0");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    // "intColumn = 5.0" should have been rewritten to "intColumn = 5" since 5.0 converts to integer value without any
    // loss of information.
    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");
  }

  @Test
  public void testEqualsIntColumnWithInvalidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5.5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testNotEqualsIntColumnWithValidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5.0");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:intColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");
  }

  @Test
  public void testNotEqualsIntColumnWithInvalidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5.5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testEqualsIntColumnWithOutOfDomainValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5000000000");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testNotEqualsIntColumnWithOutOfDomainValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5000000000");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testEqualsLongColumnWithValidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE longColumn = 5.0");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    // "intColumn = 5.0" should have been rewritten to "intColumn = 5" since 5.0 converts to integer value without any
    // loss of information.
    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");
  }

  @Test
  public void testEqualsLongColumnWithInvalidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE longColumn = 5.5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    // "intColumn = 5.5" should have been rewritten to "false" literal since an INT column can never have a decimal
    // value.
    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testNotEqualsLongColumnWithValidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE longColumn != 5.0");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    // "intColumn = 5.5" should have been rewritten to "false" literal since an INT column can never have a decimal
    // value.
    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:longColumn)), Expression(type:LITERAL, literal:<Literal longValue:5>)]))");
  }

  @Test
  public void testNotEqualsLongColumnWithInvalidDecimalValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE longColumn != 5.5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");

    System.out.println(Float.MAX_VALUE);
  }

  @Test
  public void testEqualsFloatColumnWithValidLongValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE floatColumn = 5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)]))");
  }

  @Test
  public void testEqualsFloatColumnWithInvalidLongValue() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER
        .compileToBrokerRequest("SELECT * FROM testTable WHERE floatColumn = " + String.valueOf(Long.MAX_VALUE));
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testNotEqualsFloatColumnWithValidLongValue() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER
        .compileToBrokerRequest("SELECT * FROM testTable WHERE floatColumn != " + String.valueOf(Long.MAX_VALUE));
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testNotEqualsFloatColumnWithInvalidLongValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE floatColumn != 5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:floatColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)]))");
  }

  @Test
  public void testEqualsDoubleColumnWithValidLongValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE doubleColumn = 5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)]))");
  }

  @Test
  public void testEqualsDoubleColumnWithInvalidLongValue() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER
        .compileToBrokerRequest("SELECT * FROM testTable WHERE doubleColumn = " + String.valueOf(Long.MAX_VALUE));
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testNotEqualsDoubleColumnWithValidLongValue() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER
        .compileToBrokerRequest("SELECT * FROM testTable WHERE doubleColumn != " + String.valueOf(Long.MAX_VALUE));
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testNotEqualsDoubleColumnWithInvalidLongValue() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE doubleColumn != 5");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:NOT_EQUALS, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:doubleColumn)), Expression(type:LITERAL, literal:<Literal doubleValue:5.0>)]))");
  }

  @Test
  public void testAndWithAllTrueOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5.5 AND longColumn != 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testAndWithAllFalseOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5.5 AND longColumn = 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testAndWithOneFalseOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5.5 AND longColumn != 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testOrWithAllTrueOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5.5 OR longColumn != 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }

  @Test
  public void testOrWithAllFalseOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn = 5.5 OR longColumn = 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:false>)");
  }

  @Test
  public void testOrWithOneFalseOperands() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE intColumn != 5.5 OR longColumn = 6.4");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:LITERAL, literal:<Literal boolValue:true>)");
  }
}
