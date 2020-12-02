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
package org.apache.pinot.core.geospatial.transform;

import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.geospatial.transform.function.StPointFunction;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunctionTest;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StPointFunctionTest extends BaseTransformFunctionTest {
  @Test
  public void testStPointGeogFunction() {
    testStPointFunction(0);
    testStPointFunction(1);
  }

  @Test
  public void testStPointLiteralFunction() {
    ExpressionContext expression = QueryContextConverterUtils.getExpression(String.format("ST_Point(20,10, 1)"));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    byte[][] expectedValues = new byte[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(20, 10));
      GeometryUtils.setGeography(point);
      expectedValues[i] = GeometrySerializer.serialize(point);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  private void testStPointFunction(int isGeography) {
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("ST_Point(%s,%s, %d)", DOUBLE_SV_COLUMN, DOUBLE_SV_COLUMN, isGeography));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof StPointFunction);
    Assert.assertEquals(transformFunction.getName(), StPointFunction.FUNCTION_NAME);
    byte[][] expectedValues = new byte[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(_doubleSVValues[i], _doubleSVValues[i]));
      if (isGeography > 0) {
        GeometryUtils.setGeography(point);
      }
      expectedValues[i] = GeometrySerializer.serialize(point);
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = QueryContextConverterUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("ST_Point(%s)", DOUBLE_SV_COLUMN)}, new Object[]{String.format(
        "ST_Point(%s, %s)", INT_MV_COLUMN, LONG_SV_COLUMN)}, new Object[]{String.format("st_Point(%s, %s)",
        LONG_SV_COLUMN, INT_MV_COLUMN)}};
  }
}
