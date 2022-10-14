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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
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
  public static String testScalarFunction(long randomArg1, String randomArg2) {
    return null;
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(String randomArg1, long randomArg2) {
    return null;
  }

  @ScalarFunction
  public static String testCoercionFunction(long randomArg) {
    return null;
  }

  @Test
  public void shouldMatchWithCorrectTypesAndName()
      throws NoSuchMethodException {
    // Given:
    String name = "testFunc1";
    List<RelDataType> types = FunctionTypeUtil.fromClass(long.class, String.class);

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNotNull(functionInfo);
    assertEquals(functionInfo.getMethod(),
        FunctionDefinitionRegistryTest.class.getMethod("testScalarFunction", long.class, String.class));
  }

  @Test
  public void shouldMatchWithAlternateName()
      throws NoSuchMethodException {
    // Given:
    String name = "testFunc2";
    List<RelDataType> types = FunctionTypeUtil.fromClass(long.class, String.class);

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNotNull(functionInfo);
    assertEquals(functionInfo.getMethod(),
        FunctionDefinitionRegistryTest.class.getMethod("testScalarFunction", long.class, String.class));
  }

  @Test
  public void shouldSupportTypeCoercion()
      throws NoSuchMethodException {
    // Given:
    String name = "testCoercionFunction";
    List<RelDataType> types = FunctionTypeUtil.fromClass(new Class[]{double.class});

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNotNull(functionInfo);
    assertEquals(functionInfo.getMethod(),
        FunctionDefinitionRegistryTest.class.getMethod("testCoercionFunction", long.class));
  }

  @Test
  public void shouldMatchCorrectSignature()
      throws NoSuchMethodException {
    // Given:
    String name = "testFunc1";
    List<RelDataType> types = FunctionTypeUtil.fromClass(String.class, long.class);

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNotNull(functionInfo);
    assertEquals(functionInfo.getMethod(),
        FunctionDefinitionRegistryTest.class.getMethod("testScalarFunction", String.class, long.class));
  }

  @Test
  public void shouldNotMatchWithInvalidName()
      throws NoSuchMethodException {
    // Given:
    String name = "invalidFunc";
    List<RelDataType> types = FunctionTypeUtil.fromClass(long.class, String.class);

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNull(functionInfo);
  }

  @Test
  public void shouldNotMatchWithInvalidNumberParameters()
      throws NoSuchMethodException {
    // Given:
    String name = "testFunc1";
    List<RelDataType> types = ImmutableList.of(FunctionTypeUtil.fromClass(long.class));

    // When:
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, types);

    // Then:
    assertNull(functionInfo);
  }
}
