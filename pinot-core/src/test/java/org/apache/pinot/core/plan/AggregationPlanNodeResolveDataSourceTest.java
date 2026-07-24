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
package org.apache.pinot.core.plan;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class AggregationPlanNodeResolveDataSourceTest {

  @Test
  public void testIdentifierResolvesToSegmentDataSource() {
    IndexSegment segment = mock(IndexSegment.class);
    DataSource ds = mock(DataSource.class);
    when(segment.getDataSource("col", null)).thenReturn(ds);

    ExpressionContext expr = ExpressionContext.forIdentifier("col");
    assertSame(AggregationPlanNode.resolveDataSource(expr, segment, null), ds);
  }

  @Test
  public void testLiteralReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    ExpressionContext expr = ExpressionContext.forLiteral(new LiteralContext(FieldSpec.DataType.STRING, "hello"));
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testItemFunctionWithMapDataSource() {
    IndexSegment segment = mock(IndexSegment.class);
    MapDataSource mapDs = mock(MapDataSource.class);
    DataSource keyDs = mock(DataSource.class);
    when(segment.getDataSource("mapCol", null)).thenReturn(mapDs);
    when(mapDs.getDataSource("myKey")).thenReturn(keyDs);

    ExpressionContext expr = itemExpr("mapCol", "myKey");
    assertSame(AggregationPlanNode.resolveDataSource(expr, segment, null), keyDs);
  }

  @Test
  public void testItemFunctionWithMaterializedOpenStructDataSource() {
    IndexSegment segment = mock(IndexSegment.class);
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    DataSource keyDs = mock(DataSource.class);
    when(segment.getDataSource("osCol", null)).thenReturn(osDs);
    when(osDs.isMaterialized("denseKey")).thenReturn(true);
    when(osDs.getDataSource("denseKey")).thenReturn(keyDs);

    ExpressionContext expr = itemExpr("osCol", "denseKey");
    assertSame(AggregationPlanNode.resolveDataSource(expr, segment, null), keyDs);
  }

  @Test
  public void testItemFunctionWithUnmaterializedOpenStructKeyReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(segment.getDataSource("osCol", null)).thenReturn(osDs);
    when(osDs.isMaterialized("sparseKey")).thenReturn(false);

    ExpressionContext expr = itemExpr("osCol", "sparseKey");
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testNonItemFunctionReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
        List.of(ExpressionContext.forIdentifier("a"), ExpressionContext.forIdentifier("b")));
    ExpressionContext expr = ExpressionContext.forFunction(fn);
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testItemFunctionWrongArgCountReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        List.of(ExpressionContext.forIdentifier("col")));
    ExpressionContext expr = ExpressionContext.forFunction(fn);
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testItemFunctionNonIdentifierFirstArgReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        List.of(ExpressionContext.forLiteral(new LiteralContext(FieldSpec.DataType.STRING, "notACol")),
            ExpressionContext.forLiteral(new LiteralContext(FieldSpec.DataType.STRING, "key"))));
    ExpressionContext expr = ExpressionContext.forFunction(fn);
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testItemFunctionNonLiteralSecondArgReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forIdentifier("notALiteral")));
    ExpressionContext expr = ExpressionContext.forFunction(fn);
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  @Test
  public void testItemFunctionOnPlainDataSourceReturnsNull() {
    IndexSegment segment = mock(IndexSegment.class);
    DataSource plainDs = mock(DataSource.class);
    when(segment.getDataSource("plainCol", null)).thenReturn(plainDs);

    ExpressionContext expr = itemExpr("plainCol", "key");
    assertNull(AggregationPlanNode.resolveDataSource(expr, segment, null));
  }

  private static ExpressionContext itemExpr(String column, String key) {
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        List.of(ExpressionContext.forIdentifier(column),
            ExpressionContext.forLiteral(new LiteralContext(FieldSpec.DataType.STRING, key))));
    return ExpressionContext.forFunction(fn);
  }
}
