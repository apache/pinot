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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.pinot.common.function.registry.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotFunctionRegistry;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 */
public class FunctionRegistry {
  public static final boolean CASE_SENSITIVITY = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  private FunctionRegistry() {
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  @VisibleForTesting
  public static Set<String> getRegisteredCalciteFunctionNames() {
    return PinotFunctionRegistry.getFunctionMap().map().keySet();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return PinotFunctionRegistry.getFunctionMap().containsKey(functionName, CASE_SENSITIVITY);
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParams) {
    return getFunctionInfoFromCalciteNamedMap(functionName, numParams);
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(SqlOperatorTable operatorTable, RelDataTypeFactory typeFactory,
      String functionName, List<DataSchema.ColumnDataType> argTypes) {
    PinotScalarFunction scalarFunction =
        PinotFunctionRegistry.getScalarFunction(operatorTable, typeFactory, functionName, argTypes);
    if (scalarFunction != null) {
      return scalarFunction.getFunctionInfo();
    } else {
      // TODO: convert this to throw IAE when all operator has scalar equivalent.
      return null;
    }
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromCalciteNamedMap(String functionName, int numParams) {
    List<PinotScalarFunction> candidates = PinotFunctionRegistry.getFunctionMap()
        .range(functionName, CASE_SENSITIVITY).stream()
        .filter(e -> e.getValue() instanceof PinotScalarFunction && e.getValue().getParameters().size() == numParams)
        .map(e -> (PinotScalarFunction) e.getValue()).collect(Collectors.toList());
    if (candidates.size() == 1) {
      return candidates.get(0).getFunctionInfo();
    } else {
      // TODO: convert this to throw IAE when all operator has scalar equivalent.
      return null;
    }
  }

  /**
   * Placeholders for scalar function, they register and represents the signature for transform and filter predicate
   * so that v2 engine can understand and plan them correctly.
   */
  private static class PlaceholderScalarFunctions {

    @ScalarFunction(names = {"textContains", "text_contains"}, isPlaceholder = true)
    public static boolean textContains(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"textMatch", "text_match"}, isPlaceholder = true)
    public static boolean textMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonMatch", "json_match"}, isPlaceholder = true)
    public static boolean jsonMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"vectorSimilarity", "vector_similarity"}, isPlaceholder = true)
    public static double vectorSimilarity(float[] vector1, float[] vector2) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }
  }
}
