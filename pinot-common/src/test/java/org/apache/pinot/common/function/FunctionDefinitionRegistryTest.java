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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.sql.FilterKind;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class FunctionDefinitionRegistryTest {
  private static final int MAX_NARG = 10;
  private static final List<Pattern> IGNORED_TRANSFORM_FUNCTION_SIGNATURE = ImmutableList.of(
      Pattern.compile("array.*"), // array related functions are not supported at the moment
      Pattern.compile("st_.*")// all ST GEO features are ignored.
  );
  private static final List<String> IGNORED_FUNCTION_NAMES = ImmutableList.of(
      // functions we are not supporting post transform anyway
      "valuein", "mapvalue", "inidset", "lookup", "groovy", "scalar", "geotoh3", "not_in", "timeconvert",
      // functions not needed for register b/c they are in std sql table or they will not be composed directly.
      "in", "and", "or", "range", "extract", "is_true", "is_not_true", "is_false", "is_not_false"
  );

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
  public void testCalciteFunctionMapAllRegistered() {
    Set<String> registeredCalciteFunctionNameIgnoreCase = new HashSet<>();
    for (String funcNames : FunctionRegistry.getRegisteredCalciteFunctionNames()) {
      registeredCalciteFunctionNameIgnoreCase.add(funcNames.toLowerCase());
    }
    for (TransformFunctionType enumType : TransformFunctionType.values()) {
      if (!isIgnored(enumType.getName().toLowerCase())) {
        for (String funcName : enumType.getAlternativeNames()) {
          assertTrue(registeredCalciteFunctionNameIgnoreCase.contains(funcName.toLowerCase()),
              "Unable to find transform function signature for: " + funcName);
        }
      }
    }
    for (FilterKind enumType : FilterKind.values()) {
      if (!isIgnored(enumType.name().toLowerCase())) {
        assertTrue(registeredCalciteFunctionNameIgnoreCase.contains(enumType.name().toLowerCase()),
            "Unable to find filter function signature for: " + enumType.name());
      }
    }
  }

  private boolean isIgnored(String funcName) {
    if (IGNORED_FUNCTION_NAMES.contains(funcName)) {
      return true;
    }
    for (Pattern signature : IGNORED_TRANSFORM_FUNCTION_SIGNATURE) {
      if (signature.matcher(funcName).find()) {
        return true;
      }
    }
    return false;
  }

  @ScalarFunction(names = {"testFunc1", "testFunc2"})
  public static String testScalarFunction(long randomArg1, String randomArg2) {
    return null;
  }

  @Test
  public void testScalarFunctionNames() {
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc1", 2));
    assertNotNull(FunctionRegistry.getFunctionInfo("testFunc2", 2));
    assertNull(FunctionRegistry.getFunctionInfo("testScalarFunction", 2));
    assertNull(FunctionRegistry.getFunctionInfo("testFunc1", 1));
    assertNull(FunctionRegistry.getFunctionInfo("testFunc2", 1));
  }
}
