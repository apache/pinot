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
package org.apache.pinot.common.function;

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FunctionDefinitionRegistryTest {

  @Test
  public void testIsAggFunc() {
    assertTrue(AggregationFunctionType.isAggregationFunction("count"));
    assertTrue(AggregationFunctionType.isAggregationFunction("percentileRawEstMV"));
    assertTrue(AggregationFunctionType.isAggregationFunction("PERCENTILERAWESTMV"));
    assertTrue(AggregationFunctionType.isAggregationFunction("percentilerawestmv"));
    assertTrue(AggregationFunctionType.isAggregationFunction("percentile_raw_est_mv"));
    assertTrue(AggregationFunctionType.isAggregationFunction("PERCENTILE_RAW_EST_MV"));
    assertTrue(AggregationFunctionType.isAggregationFunction("PERCENTILEEST90"));
    assertTrue(AggregationFunctionType.isAggregationFunction("percentileest90"));
    assertFalse(AggregationFunctionType.isAggregationFunction("toEpochSeconds"));
  }

  @Test
  public void testIsTransformFunc() {
    assertTrue(TransformFunctionType.isTransformFunction("toEpochSeconds"));
    assertTrue(TransformFunctionType.isTransformFunction("json_extract_scalar"));
    assertTrue(TransformFunctionType.isTransformFunction("jsonextractscalar"));
    assertTrue(TransformFunctionType.isTransformFunction("JSON_EXTRACT_SCALAR"));
    assertTrue(TransformFunctionType.isTransformFunction("JSONEXTRACTSCALAR"));
    assertTrue(TransformFunctionType.isTransformFunction("jsonExtractScalar"));
    assertTrue(TransformFunctionType.isTransformFunction("ST_AsText"));
    assertTrue(TransformFunctionType.isTransformFunction("STAsText"));
    assertTrue(TransformFunctionType.isTransformFunction("stastext"));
    assertTrue(TransformFunctionType.isTransformFunction("ST_ASTEXT"));
    assertTrue(TransformFunctionType.isTransformFunction("STASTEXT"));
    assertFalse(TransformFunctionType.isTransformFunction("foo_bar"));
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(Long randomArg1, String randomArg2) {
    return null;
  }

  @ScalarFunction(names = {"testFunc2"})
  public static String testScalarFunction2(Long randomArg1, Long randomArg2) {
    return null;
  }

  @Test
  public void testScalarFunctionNames() {
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc1", 2, ""));
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc2", 2, ""));
    assertNull(FunctionRegistry.getFunctionInfo("testScalarFunction", 2, ""));
    assertNull(FunctionRegistry.getFunctionInfo("testFunc1", 1, ""));
    assertNull(FunctionRegistry.getFunctionInfo("testFunc2", 1, ""));
    FunctionInfo func2LongString =
        FunctionRegistry.getFunctionInfo("testFunc2", 2, "java.lang.Long,java.lang.String");
    assertNotNull(func2LongString);
    Class[] paramTypes = func2LongString.getMethod().getParameterTypes();
    assertEquals(paramTypes.length, 2);
    assertEquals(paramTypes[0].getTypeName(), "java.lang.Long");
    assertEquals(paramTypes[1].getTypeName(), "java.lang.String");
    FunctionInfo func2LongLong = FunctionRegistry.getFunctionInfo("testFunc2", 2, "java.lang.Long,java.lang.Long");
    assertNotNull(func2LongLong);
    Class[] paramTypes2 = func2LongLong.getMethod().getParameterTypes();
    assertEquals(paramTypes2.length, 2);
    assertEquals(paramTypes2[0].getTypeName(), "java.lang.Long");
    assertEquals(paramTypes2[1].getTypeName(), "java.lang.Long");
  }
}
