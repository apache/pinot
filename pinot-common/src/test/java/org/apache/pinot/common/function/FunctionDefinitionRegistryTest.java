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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec;
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
  public static String testScalarFunction(long randomArg1, String randomArg2) {
    return null;
  }

  @ScalarFunction(names = {"testFunc2"})
  public static String testScalarFunction2(long randomArg1, Long randomArg2) {
    return null;
  }

  // Test that function matches with parameter counts
  @Test
  public void testScalarFunctionNamesMatch() {
    List<FieldSpec.DataType> funcArgs1 = new ArrayList<>();
    funcArgs1.add(FieldSpec.DataType.LONG);
    funcArgs1.add(FieldSpec.DataType.INT);
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc1", funcArgs1));
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc2", funcArgs1));
    assertNull(FunctionRegistry.getFunctionInfo("testScalarFunction", funcArgs1));
    List<FieldSpec.DataType> funcArgs2 = new ArrayList<>();
    funcArgs2.add(FieldSpec.DataType.LONG);
    assertNull(FunctionRegistry.getFunctionInfo("testFunc1", funcArgs2));
    assertNull(FunctionRegistry.getFunctionInfo("testFunc2", funcArgs2));
  }

  // Test that function register can do type matching
  @Test
  public void testScalarFunctionTypeMatch() {
    List<FieldSpec.DataType> funcArgs3 = new ArrayList<>();
    funcArgs3.add(FieldSpec.DataType.LONG);
    funcArgs3.add(FieldSpec.DataType.LONG);
    FunctionInfo func2LongLong = FunctionRegistry.getFunctionInfo("testFunc2", funcArgs3);
    assertNotNull(func2LongLong);
    Class[] paramTypes = func2LongLong.getMethod().getParameterTypes();
    assertEquals(paramTypes.length, 2);
    assertEquals(paramTypes[0].getTypeName(), "long");
    assertEquals(paramTypes[1].getTypeName(), "java.lang.Long");
    List<FieldSpec.DataType> funcArgs1 = new ArrayList<>();
    funcArgs1.add(FieldSpec.DataType.LONG);
    funcArgs1.add(FieldSpec.DataType.STRING);
    FunctionInfo func2LongString = FunctionRegistry.getFunctionInfo("testFunc2", funcArgs1);
    assertNotNull(func2LongString);
    Class[] paramTypes2 = func2LongString.getMethod().getParameterTypes();
    assertEquals(paramTypes2.length, 2);
    assertEquals(paramTypes2[0].getTypeName(), "long");
    assertEquals(paramTypes2[1].getTypeName(), "java.lang.String");
  }
}
