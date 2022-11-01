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
import java.util.regex.Pattern;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
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
      "valuein", "mapvalue", "inidset", "lookup", "groovy", "scalar", "geotoh3", "case", "not_in",
      // functions will not occur post transform as function since they are std operator tables
      "in", "and", "or", "not"
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
  public void testAllRegistered() {
    for (TransformFunctionType enumType : TransformFunctionType.values()) {
      if (!isIgnored(enumType.getName().toLowerCase())) {
        for (String funcName : enumType.getAliases()) {
          boolean foundSignature = false;
          for (int nArg = 0; nArg < MAX_NARG; nArg++) {
            if (FunctionRegistry.getFunctionInfo(funcName, nArg) != null) {
              foundSignature = true;
              break;
            }
          }
          assertTrue(foundSignature, "Unable to find transform/filter function signature for: " + funcName);
        }
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
