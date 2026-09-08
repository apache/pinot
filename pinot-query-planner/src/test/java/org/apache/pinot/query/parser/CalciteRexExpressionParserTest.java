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
package org.apache.pinot.query.parser;

import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests UUID literal conversion from multi-stage Rex expressions to single-stage request expressions.
public class CalciteRexExpressionParserTest {
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  @Test
  public void testUuidLiteralUsesBinaryValue() {
    RexExpression.Literal uuidLiteral =
        new RexExpression.Literal(ColumnDataType.UUID, new ByteArray(UuidUtils.toBytes(UUID_VALUE)));

    Literal literal = CalciteRexExpressionParser.toLiteral(uuidLiteral);

    assertTrue(literal.isSetBinaryValue());
    assertEquals(literal.getBinaryValue(), UuidUtils.toBytes(UUID_VALUE));
  }

  @Test
  public void testBytesArrayLiteralUsesBytesArrayValue() {
    byte[][] expected = {{0}, {1, 2}};
    ByteArray[] internalValue = {new ByteArray(expected[0]), new ByteArray(expected[1])};
    RexExpression.Literal bytesArrayLiteral = new RexExpression.Literal(ColumnDataType.BYTES_ARRAY, internalValue);

    Literal literal = CalciteRexExpressionParser.toLiteral(bytesArrayLiteral);

    assertTrue(literal.isSetBytesArrayValue());
    assertEquals(RequestUtils.getBytesArrayValue(literal), expected);
  }
}
