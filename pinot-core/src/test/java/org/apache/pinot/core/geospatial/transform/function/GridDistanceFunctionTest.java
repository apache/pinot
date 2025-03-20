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

import com.uber.h3core.H3CoreV3;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunctionTest;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;


public class GridDistanceFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testGridDistance()
      throws IOException {
    H3CoreV3 h3CoreV3 = H3CoreV3.newInstance();
    // Test points in San Francisco
    double lat1 = 37.7749;
    double lon1 = -122.4194;
    double lat2 = 37.7833;
    double lon2 = -122.4167;
    int resolution = 9;

    // Convert points to H3 indexes
    long h3Index1 = h3CoreV3.geoToH3(lat1, lon1, resolution);
    long h3Index2 = h3CoreV3.geoToH3(lat2, lon2, resolution);

    // Calculate expected grid distance
    long expectedDistance = 0;
    for (int k = 1; k <= 100; k++) {
      java.util.List<java.util.List<Long>> ringsWithDistances = h3CoreV3.kRingDistances(h3Index1, k);
      boolean found = false;
      for (int distance = 0; distance < ringsWithDistances.size(); distance++) {
        if (ringsWithDistances.get(distance).contains(h3Index2)) {
          expectedDistance = distance;
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }

    // Create an array of expected values with size NUM_ROWS
    long[] expectedValues = new long[NUM_ROWS];
    Arrays.fill(expectedValues, expectedDistance);

    // Test the function
    ExpressionContext expression = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "gridDistance",
            Arrays.asList(ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index1),
                ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index2))));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    testTransformFunction(transformFunction, expectedValues);

    // Test with same point (distance should be 0)
    Arrays.fill(expectedValues, 0);
    expression = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "gridDistance",
        Arrays.asList(ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index1),
            ExpressionContext.forLiteral(FieldSpec.DataType.LONG, h3Index1))));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    testTransformFunction(transformFunction, expectedValues);
  }
}
