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
package org.apache.pinot.core.query.postaggregation;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PostAggregationFunctionTest {

  @Test
  public void testPostAggregationFunction() {
    // Plus
    FunctionContext functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "plus",
        ImmutableList.of(ExpressionContext.forLiteral(1), ExpressionContext.forLiteral(2L)));
    PostAggregationFunction function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{1, 2L}), 3.0);

    // Minus
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "MINUS",
        ImmutableList.of(ExpressionContext.forLiteral(3f), ExpressionContext.forLiteral(4.0)));
    function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{3f, 4.0}), -1.0);

    // Times
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "tImEs",
        ImmutableList.of(ExpressionContext.forLiteral("5"), ExpressionContext.forLiteral(6)));
    function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{"5", 6}), 30.0);

    // Reverse
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "reverse",
        ImmutableList.of(ExpressionContext.forLiteral(new Object[]{"1234567890"})));
    function = new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.LONG});
    assertEquals(function.getResultType(), ColumnDataType.STRING);
    assertEquals(function.invoke(new Object[]{"1234567890"}), "0987654321");

    // ST_AsText
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "ST_AsText",
        ImmutableList.of(ExpressionContext.forLiteral(new Object[]{
            GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20)))
        })));
    function = new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.BYTES});
    assertEquals(function.getResultType(), ColumnDataType.STRING);
    assertEquals(function.invoke(
            new Object[]{GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10,
                20)))}),
        "POINT (10 20)");

    // Cast
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "cast",
        ImmutableList.of(ExpressionContext.forLiteral(1), ExpressionContext.forLiteral("LONG")));
    function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    assertEquals(function.getResultType(), ColumnDataType.OBJECT);
    assertEquals(function.invoke(new Object[]{1, "LONG"}), 1L);

    // Boolean literals equal
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "equals",
        ImmutableList.of(ExpressionContext.forLiteral(true), ExpressionContext.forLiteral(true)));
    function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN});
    assertEquals(function.getResultType(), ColumnDataType.BOOLEAN);
    assertEquals(function.invoke(new Object[]{true, true}), true);

    // Boolean literals notEquals
    functionCtx = new FunctionContext(FunctionContext.Type.TRANSFORM, "notEquals",
        ImmutableList.of(ExpressionContext.forLiteral(true), ExpressionContext.forLiteral(true)));
    function =
        new PostAggregationFunction(functionCtx, new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN});
    assertEquals(function.getResultType(), ColumnDataType.BOOLEAN);
    assertEquals(function.invoke(new Object[]{true, true}), false);
  }
}
