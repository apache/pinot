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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
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
  public static String testScalarFunction(long randomArg1) {
    return null;
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(int randomArg1) {
    return null;
  }

  @ScalarFunction(nullableParameters = true)
  public static String testNullableFun(Long randomArg1, String randomArg2) {
    return null;
  }

  @ScalarFunction(nullableParameters = true)
  public static String testNullableFun(Long randomArg1, Double randomArg2) {
    return null;
  }

  @Test
  public void shouldRespectCustomFunctionNames() {
    // Given:
    List<Class<?>> types = ImmutableList.of(long.class, String.class);

    // Then:
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc1", types));
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc2", types));
    assertNull(FunctionRegistry.getFunctionInfo("testScalarFunction", types));
  }

  @Test
  public void shouldReturnCorrectFunctionWithPolymorphism() {
    // Given:
    List<Class<?>> types = ImmutableList.of(long.class, String.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testFunc1", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), types);
  }

  @Test
  public void shouldReturnCorrectFunctionWithPrimitiveTypeCasts() {
    // Given:
    List<Class<?>> types = ImmutableList.of(Long.class, String.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testFunc1", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), ImmutableList.of(long.class, String.class));
  }

  @Test
  public void shouldReturnCorrectFunctionWithPrimitiveWidening() {
    // Given:
    List<Class<?>> types = ImmutableList.of(int.class, String.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testFunc1", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), ImmutableList.of(long.class, String.class));
  }

  @Test
  public void shouldReturnExactMatchIfMultipleCouldMatch() {
    // Given:
    List<Class<?>> types = ImmutableList.of(long.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testFunc1", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), types);
  }

  @Test
  public void shouldThrowIfMultipleMatches() {
    // Given:
    List<Class<?>> types = ImmutableList.of(short.class); // widens to both int/long

    // When:
    Assert.assertThrows(BadQueryRequestException.class,
        () -> FunctionRegistry.getFunctionInfo("testFunc1", types));
  }

  @Test
  public void shouldReturnCorrectFunctionWithPrimitiveWideningAndUnwrapping() {
    // Given:
    List<Class<?>> types = ImmutableList.of(Integer.class, String.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testFunc1", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), ImmutableList.of(long.class, String.class));
  }

  @Test
  public void shouldRespectNullableParams() {
    // Given:
    List<Class<?>> types = new ArrayList<>();
    types.add(null);
    types.add(String.class);

    // When:
    FunctionInfo fun = FunctionRegistry.getFunctionInfo("testNullableFun", types);

    // Then:
    assertNotNull(fun);
    assertEquals(fun.getParameterTypes(), ImmutableList.of(Long.class, String.class));
  }
}
