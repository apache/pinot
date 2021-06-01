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
package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JsonStatementOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn", FieldSpec.DataType.INT)
          .addSingleValueDimension("longColumn", FieldSpec.DataType.LONG)
          .addSingleValueDimension("stringColumn", FieldSpec.DataType.STRING)
          .addMultiValueDimension("jsonColumn", FieldSpec.DataType.JSON).build();

  /** Test that a json path expression in SELECT list is properly converted to a JSON_EXTRACT_SCALAR function within an AS function. */
  @Test
  public void testJsonSelect() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER.compileToBrokerRequest("SELECT jsonColumn.x FROM testTable");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.x>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn.x))]))");
  }

  /** Test that a predicate comparing a json path expression with STRING literal is properly converted into a JSON_MATCH function. */
  @Test
  public void testJsonStringFilter() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy'");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_MATCH, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:\"$.name.first\" = 'daffy'>)]))");
  }

  /** Test that a predicate comparing a json path expression with number literal is properly converted into a JSON_MATCH function. */
  @Test
  public void testJsonNumericalFilter() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT * FROM testTable WHERE jsonColumn.id = 101");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_MATCH, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:\"$.id\" = 101>)]))");
  }

  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupBy() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.id>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn.id))]))");

    Assert.assertEquals(pinotQuery.getGroupByList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.id>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)]))");
  }

  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupByHaving() {

    // Test HAVING clause with a STRING value extracted using json path expression.
    BrokerRequest sqlBrokerRequest1 =
        SQL_COMPILER.compileToBrokerRequest("SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name.last = 'mouse'");
    PinotQuery pinotQuery1 = sqlBrokerRequest1.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery1, SCHEMA);

    Assert.assertEquals(pinotQuery1.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn.name.last))]))");

    Assert.assertEquals(pinotQuery1.getGroupByList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)]))");

    Assert.assertEquals(pinotQuery1.getHavingExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:LITERAL, literal:<Literal stringValue:mouse>)]))");

    // Test HAVING clause with a DOUBLE value extract using json path expression.
    BrokerRequest sqlBrokerRequest2 =
        SQL_COMPILER.compileToBrokerRequest("SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name.last = 'mouse'");
    PinotQuery pinotQuery2 = sqlBrokerRequest2.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery2, SCHEMA);

    Assert.assertEquals(pinotQuery2.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn.name.last))]))");

    Assert.assertEquals(pinotQuery2.getGroupByList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)]))");

    Assert.assertEquals(pinotQuery2.getHavingExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:EQUALS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:LITERAL, literal:<Literal stringValue:mouse>)]))");
  }

  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test
  public void testJsonSelectFilterGroupBy() {
    BrokerRequest sqlBrokerRequest = SQL_COMPILER.compileToBrokerRequest(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)])), Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn.name.last))]))");

    Assert.assertEquals(pinotQuery.getFilterExpression().toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_MATCH, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:\"$.id\" = 101>)]))");

    Assert.assertEquals(pinotQuery.getGroupByList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.last>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)]))");
  }

  /** Test an aggregation function over json path expression in SELECT clause. */
  @Test
  public void testStringFunctionOverJsonPathSelectExpression() {
    BrokerRequest sqlBrokerRequest =
        SQL_COMPILER.compileToBrokerRequest("SELECT UPPER(jsonColumn.name.first) FROM testTable");
    PinotQuery pinotQuery = sqlBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Assert.assertEquals(pinotQuery.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:UPPER, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.name.first>), Expression(type:LITERAL, literal:<Literal stringValue:STRING>), Expression(type:LITERAL, literal:<Literal stringValue:null>)]))])), Expression(type:IDENTIFIER, identifier:Identifier(name:upper(jsonColumn.name.first)))]))");
  }

  /** Test a numerical function over json path expression in SELECT clause. */
  @Test
  public void testNumericalFunctionOverJsonPathSelectExpression() {

    // Test without user-specified alias.
    BrokerRequest sqlBrokerRequest1 =
        SQL_COMPILER.compileToBrokerRequest("SELECT MAX(jsonColumn.id) FROM testTable");
    PinotQuery pinotQuery1 = sqlBrokerRequest1.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery1, SCHEMA);

    Assert.assertEquals(pinotQuery1.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:MAX, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.id>), Expression(type:LITERAL, literal:<Literal stringValue:DOUBLE>), Expression(type:LITERAL, literal:<Literal doubleValue:-Infinity>)]))])), Expression(type:IDENTIFIER, identifier:Identifier(name:max(jsonColumn.id)))]))");

    // Test with user-specified alias.
    BrokerRequest sqlBrokerRequest2 =
        SQL_COMPILER.compileToBrokerRequest("SELECT MAX(jsonColumn.id) AS x FROM testTable");
    PinotQuery pinotQuery2 = sqlBrokerRequest2.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery2, SCHEMA);

    Assert.assertEquals(pinotQuery2.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:MAX, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.id>), Expression(type:LITERAL, literal:<Literal stringValue:DOUBLE>), Expression(type:LITERAL, literal:<Literal doubleValue:-Infinity>)]))])), Expression(type:IDENTIFIER, identifier:Identifier(name:x))]))");

    // Test with nested function calls (minus function being used within max function).
    BrokerRequest sqlBrokerRequest3 =
        SQL_COMPILER.compileToBrokerRequest("SELECT MAX(jsonColumn.id - 5) FROM testTable");
    PinotQuery pinotQuery3 = sqlBrokerRequest3.getPinotQuery();
    OPTIMIZER.optimize(pinotQuery3, SCHEMA);

    Assert.assertEquals(pinotQuery3.getSelectList().get(0).toString(),
        "Expression(type:FUNCTION, functionCall:Function(operator:AS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:MAX, operands:[Expression(type:FUNCTION, functionCall:Function(operator:MINUS, operands:[Expression(type:FUNCTION, functionCall:Function(operator:JSON_EXTRACT_SCALAR, operands:[Expression(type:IDENTIFIER, identifier:Identifier(name:jsonColumn)), Expression(type:LITERAL, literal:<Literal stringValue:$.id>), Expression(type:LITERAL, literal:<Literal stringValue:DOUBLE>), Expression(type:LITERAL, literal:<Literal doubleValue:-Infinity>)])), Expression(type:LITERAL, literal:<Literal longValue:5>)]))])), Expression(type:IDENTIFIER, identifier:Identifier(name:max(minus(jsonColumn.id,'5'))))]))");
  }
}
