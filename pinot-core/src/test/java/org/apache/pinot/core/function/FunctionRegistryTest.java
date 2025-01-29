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
package org.apache.pinot.core.function;

import java.util.EnumSet;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.sql.FilterKind;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


// NOTE: Keep this test in pinot-core to include all built-in scalar functions.
public class FunctionRegistryTest {
  // TODO: Support these functions
  private static final EnumSet<TransformFunctionType> IGNORED_TRANSFORM_FUNCTION_TYPES = EnumSet.of(
      // Special placeholder functions without implementation
      TransformFunctionType.SCALAR,
      // Special functions that requires index
      TransformFunctionType.JSON_EXTRACT_INDEX, TransformFunctionType.MAP_VALUE, TransformFunctionType.LOOKUP,
      // TODO: Support these functions
      TransformFunctionType.IN, TransformFunctionType.NOT_IN, TransformFunctionType.IS_TRUE,
      TransformFunctionType.IS_NOT_TRUE, TransformFunctionType.IS_FALSE, TransformFunctionType.IS_NOT_FALSE,
      TransformFunctionType.AND, TransformFunctionType.OR, TransformFunctionType.JSON_EXTRACT_SCALAR,
      TransformFunctionType.JSON_EXTRACT_KEY, TransformFunctionType.TIME_CONVERT,
      TransformFunctionType.DATE_TIME_CONVERT_WINDOW_HOP, TransformFunctionType.ARRAY_LENGTH,
      TransformFunctionType.ARRAY_AVERAGE, TransformFunctionType.ARRAY_MIN, TransformFunctionType.ARRAY_MAX,
      TransformFunctionType.ARRAY_SUM, TransformFunctionType.VALUE_IN, TransformFunctionType.IN_ID_SET,
      TransformFunctionType.GROOVY, TransformFunctionType.CLP_DECODE, TransformFunctionType.CLP_ENCODED_VARS_MATCH,
      TransformFunctionType.ST_POLYGON, TransformFunctionType.ST_AREA, TransformFunctionType.ITEM,
      TransformFunctionType.TIME_SERIES_BUCKET);
  private static final EnumSet<FilterKind> IGNORED_FILTER_KINDS = EnumSet.of(
      // Special filter functions without implementation
      FilterKind.TEXT_MATCH, FilterKind.TEXT_CONTAINS, FilterKind.JSON_MATCH, FilterKind.VECTOR_SIMILARITY,
      // TODO: Support these functions
      FilterKind.AND, FilterKind.OR, FilterKind.RANGE, FilterKind.IN, FilterKind.NOT_IN);

  @Test
  public void testTransformAndFilterFunctionsRegistered() {
    for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
      if (IGNORED_TRANSFORM_FUNCTION_TYPES.contains(transformFunctionType)) {
        continue;
      }
      for (String name : transformFunctionType.getNames()) {
        assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize(name)),
            "Unable to find transform function signature for: " + name);
      }
    }
    for (FilterKind filterKind : FilterKind.values()) {
      if (IGNORED_FILTER_KINDS.contains(filterKind)) {
        continue;
      }
      assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize(filterKind.name())),
          "Unable to find filter function signature for: " + filterKind);
    }
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(long randomArg1, String randomArg2) {
    return null;
  }

  @Test
  public void testScalarFunctionNames() {
    assertNotNull(FunctionRegistry.lookupFunctionInfo("testfunc1", 2));
    assertNotNull(FunctionRegistry.lookupFunctionInfo("testfunc2", 2));
    assertNull(FunctionRegistry.lookupFunctionInfo("testscalarfunction", 2));
    assertNull(FunctionRegistry.lookupFunctionInfo("testfunc1", 1));
    assertNull(FunctionRegistry.lookupFunctionInfo("testfunc2", 1));
  }
}
