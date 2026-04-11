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
package org.apache.pinot.common.request.context;

import java.util.List;
import java.util.Locale;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests filter conversion for literal-only expressions on the right-hand side.
 */
public class RequestContextUtilsTest {
  private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440001";

  @Test
  public void testGetFilterWithUuidCastLiteralRhs() {
    FilterContext filter =
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("uuidCol = CAST('" + UUID_1 + "' AS UUID)"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    EqPredicate predicate = (EqPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValue(), UUID_1);
  }

  @Test
  public void testGetFilterExpressionContextWithUuidCastLiteralIn() {
    FilterContext filter = RequestContextUtils.getFilter(
        RequestContextUtils.getExpression("uuidCol IN (CAST('" + UUID_1 + "' AS UUID), CAST('" + UUID_2 + "' AS UUID))"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    InPredicate predicate = (InPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValues(), List.of(UUID_1, UUID_2));
  }

  @Test
  public void testGetFilterWithToUuidLiteralRhsNormalizesCase() {
    FilterContext filter = RequestContextUtils.getFilter(
        CalciteSqlParser.compileToExpression("uuidCol = TO_UUID('550E8400-E29B-41D4-A716-446655440000')"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    EqPredicate predicate = (EqPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValue(), UUID_1);
  }

  @Test
  public void testGetFilterWithUuidToBytesNestedLiteralRhs() {
    FilterContext filter = RequestContextUtils.getFilter(
        CalciteSqlParser.compileToExpression("bytesCol = UUID_TO_BYTES(CAST('" + UUID_1 + "' AS UUID))"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    EqPredicate predicate = (EqPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "bytesCol");
    Assert.assertEquals(predicate.getValue(), BytesUtils.toHexString(UuidUtils.toBytes(UUID_1)));
  }

  @Test
  public void testGetFilterWithBytesToUuidNestedLiteralRhs() {
    String bytesLiteral = BytesUtils.toHexString(UuidUtils.toBytes(UUID_2));
    FilterContext filter = RequestContextUtils.getFilter(
        CalciteSqlParser.compileToExpression("uuidCol = BYTES_TO_UUID(CAST('" + bytesLiteral + "' AS BYTES))"));

    Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
    EqPredicate predicate = (EqPredicate) filter.getPredicate();
    Assert.assertEquals(predicate.getLhs().getIdentifier(), "uuidCol");
    Assert.assertEquals(predicate.getValue(), UUID_2);
  }

  @Test
  public void testUuidCastLiteralUsesLocaleIndependentTypeParsing() {
    Locale originalDefault = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    try {
      FilterContext filter = RequestContextUtils.getFilter(
          CalciteSqlParser.compileToExpression("uuidCol = CAST('" + UUID_1 + "' AS uuid)"));

      Assert.assertEquals(filter.getType(), FilterContext.Type.PREDICATE);
      EqPredicate predicate = (EqPredicate) filter.getPredicate();
      Assert.assertEquals(predicate.getValue(), UUID_1);
    } finally {
      Locale.setDefault(originalDefault);
    }
  }

  @Test
  public void testInvalidUuidFunctionArityThrowsBadQueryRequest() {
    FunctionContext castFunction = new FunctionContext(FunctionContext.Type.TRANSFORM, "cast",
        List.of(ExpressionContext.forLiteral(DataType.STRING, UUID_1)));
    ExpressionContext filterExpression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "equals",
            List.of(ExpressionContext.forIdentifier("uuidCol"), ExpressionContext.forFunction(castFunction))));

    BadQueryRequestException exception =
        Assert.expectThrows(BadQueryRequestException.class, () -> RequestContextUtils.getFilter(filterExpression));
    Assert.assertEquals(exception.getMessage(), "CAST function must have exactly 2 operands");
  }
}
