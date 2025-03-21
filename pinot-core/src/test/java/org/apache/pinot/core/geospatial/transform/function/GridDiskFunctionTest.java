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
package org.apache.pinot.core.geospatial.transform.function;

import com.uber.h3core.H3Core;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunctionTest;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GridDiskFunctionTest extends BaseTransformFunctionTest {

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testGridDisk()
      throws IOException {
    H3Core h3Core = H3Core.newInstance();
    // Test point in San Francisco
    double lat = 37.7749;
    double lng = -122.4194;
    long h3Index = h3Core.latLngToCell(lat, lng, 9);

    // Test with k=1 (immediate neighbors)
    int k = 1;
    List<Long> expectedDisk = h3Core.gridDisk(h3Index, k);
    long[][] expectedValues = new long[NUM_ROWS][];
    Arrays.fill(expectedValues, expectedDisk.stream().mapToLong(Long::longValue).toArray());

    // Create the transform function
    ExpressionContext h3Context = ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index);
    ExpressionContext kContext = ExpressionContext.forLiteral(FieldSpec.DataType.INT, k);
    TransformFunction transformFunction = TransformFunctionFactory.get(
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "gridDisk",
            Arrays.asList(h3Context, kContext))), _dataSourceMap);
    Map<String, ColumnContext> columnContextMap = new HashMap<>();
    for (Map.Entry<String, DataSource> entry : _dataSourceMap.entrySet()) {
      columnContextMap.put(entry.getKey(), ColumnContext.fromDataSource(entry.getValue()));
    }
    transformFunction.init(Arrays.asList(
        TransformFunctionFactory.get(h3Context, _dataSourceMap),
        TransformFunctionFactory.get(kContext, _dataSourceMap)), columnContextMap);
    testTransformFunctionMV(transformFunction, expectedValues);

    // Test with k=0 (only the center)
    k = 0;
    expectedDisk = h3Core.gridDisk(h3Index, k);
    expectedValues = new long[NUM_ROWS][];
    Arrays.fill(expectedValues, expectedDisk.stream().mapToLong(Long::longValue).toArray());

    h3Context = ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index);
    kContext = ExpressionContext.forLiteral(FieldSpec.DataType.INT, k);
    transformFunction = TransformFunctionFactory.get(
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "gridDisk",
            Arrays.asList(h3Context, kContext))), _dataSourceMap);
    transformFunction.init(Arrays.asList(
        TransformFunctionFactory.get(h3Context, _dataSourceMap),
        TransformFunctionFactory.get(kContext, _dataSourceMap)), columnContextMap);
    testTransformFunctionMV(transformFunction, expectedValues);

    // Test with k=2 (two rings of neighbors)
    k = 2;
    expectedDisk = h3Core.gridDisk(h3Index, k);
    expectedValues = new long[NUM_ROWS][];
    Arrays.fill(expectedValues, expectedDisk.stream().mapToLong(Long::longValue).toArray());

    h3Context = ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index);
    kContext = ExpressionContext.forLiteral(FieldSpec.DataType.INT, k);
    transformFunction = TransformFunctionFactory.get(
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "gridDisk",
            Arrays.asList(h3Context, kContext))), _dataSourceMap);
    transformFunction.init(Arrays.asList(
        TransformFunctionFactory.get(h3Context, _dataSourceMap),
        TransformFunctionFactory.get(kContext, _dataSourceMap)), columnContextMap);
    testTransformFunctionMV(transformFunction, expectedValues);
  }
}
