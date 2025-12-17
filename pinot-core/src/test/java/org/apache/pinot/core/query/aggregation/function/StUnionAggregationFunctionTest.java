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
package org.apache.pinot.core.query.aggregation.function;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryCombiner;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class StUnionAggregationFunctionTest {
  @Test
  public void testMergeHandlesGeometryCollectionInputs()
      throws Exception {
    ExpressionContext expression = ExpressionContext.forIdentifier("geometryColumn");
    StUnionAggregationFunction aggregationFunction = new StUnionAggregationFunction(List.of(expression));
    Geometry polygon = GeometryUtils.GEOMETRY_WKT_READER.read("POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))");
    Geometry line = GeometryUtils.GEOMETRY_WKT_READER.read("LINESTRING(20 20, 25 25)");
    Geometry geometryCollection =
        GeometryUtils.GEOMETRY_WKT_READER.read("GEOMETRYCOLLECTION (POINT (30 30), LINESTRING (35 35, 40 40))");

    Geometry intermediate = aggregationFunction.merge(polygon, line);
    Geometry result = aggregationFunction.merge(intermediate, geometryCollection);

    Geometry expected = UnaryUnionOp.union(GeometryCombiner.combine(polygon, line, geometryCollection));
    assertEquals(result, expected);
  }

  @Test
  public void testAggregateHandlesMixedDimensionSequence()
      throws Exception {
    ExpressionContext expression = ExpressionContext.forIdentifier("geometryColumn");
    StUnionAggregationFunction aggregationFunction = new StUnionAggregationFunction(List.of(expression));

    Geometry polygon = GeometryUtils.GEOMETRY_WKT_READER.read("POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))");
    Geometry line = GeometryUtils.GEOMETRY_WKT_READER.read("LINESTRING(10 10, 15 15)");
    Geometry geometryCollection =
        GeometryUtils.GEOMETRY_WKT_READER.read("GEOMETRYCOLLECTION (POINT (20 20), LINESTRING (25 25, 30 30))");

    byte[][] values = new byte[][]{
        GeometrySerializer.serialize(polygon),
        GeometrySerializer.serialize(line),
        GeometrySerializer.serialize(geometryCollection)
    };

    AggregationResultHolder aggregationResultHolder = aggregationFunction.createAggregationResultHolder();
    Map<ExpressionContext, BlockValSet> blockValSetMap =
        Collections.singletonMap(expression, new BytesBlockValSet(values));

    aggregationFunction.aggregate(values.length, aggregationResultHolder, blockValSetMap);

    Geometry expected = UnaryUnionOp.union(GeometryCombiner.combine(polygon, line, geometryCollection));
    assertEquals(aggregationResultHolder.getResult(), expected);
  }

  private static class BytesBlockValSet extends SyntheticBlockValSets.Base {
    private final byte[][] _values;

    private BytesBlockValSet(byte[][] values) {
      _values = values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.BYTES;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      return _values;
    }
  }
}
