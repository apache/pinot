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
package org.apache.pinot.core.udf;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.spi.annotations.ScalarFunction;


/// The Udf interface represents a User Defined Function (UDF) in Pinot.
///
/// In Pinot UDFs can be either [@ScalarFunction][org.apache.pinot.spi.annotations.ScalarFunction] or
///  [TransformFunction][org.apache.pinot.core.operator.transform.function.TransformFunction].
/// The first are row based (are called once per row) and the second are block based (are called once per block), which
/// makes them more efficient for large datasets.
///
/// These functions can be used in different parts of the Pinot query processing pipeline. For example,
/// TransformFunctions are when ProjectPlanNodes in SSE are materialized into TransformOpeartors, while scalar functions
/// are used mostly everywhere else, such as in filter expressions or even project nodes in MSE.
/// But although a ScalarFunction can be wrapped into a TransformFunction using ScalarTransformFunctionWrapper,
/// TransformFunctions cannot be used in places where ScalarFunctions are expected.
///
/// Therefore in order to add a new function, one should always implement the ScalarFunction interface, and if
/// TransformFunction is needed, it can be implemented as a wrapper around the ScalarFunction. But this was not
/// automatically enforced by the APIs.
///
/// This is why the Udf function was introduced. Udf interfaces should be the actual way to register an UDF in Pinot.
/// This interface is used to provide a unified way to describe UDFs, including their main function name,
/// description, examples and in future it could be used to register functions in TransformFunctionFactory and
/// FunctionRegistry (which is the one used to look for scalar functions).
///
/// The examples are used to provide a set of examples for the function, which can be used in documentation or testing.
public abstract class Udf {

  /// The main function name of the UDF.
  ///
  /// This is treated as an ID, which means that on a single Pinot process there should be only one UDF with a given
  /// main function name.
  public abstract String getMainName();

  /// Returns the main function name of the UDF, canonicalized as defined in [FunctionRegistry#canonicalize].
  /// This is used to ensure that the function name is in a consistent format, which is important for
  /// function registration, lookup and reporting.
  public String getMainCanonicalName() {
    return FunctionRegistry.canonicalize(getMainName());
  }

  /// A set with all names of the functions that this UDF can be called with, including the main name.
  ///
  /// This is used to support different aliases for the same function, so that users can call the function.
  public Set<String> getAllNames() {
    return Set.of(getMainName());
  }

  /// Returns a set with all names of the functions that this UDF can be called with, including the main name,
  /// canonicalized as defined in [FunctionRegistry#canonicalize].
  ///
  /// This is used to ensure that the function names are in a consistent format, which is important for
  /// function registration, lookup and reporting.
  public Set<String> getAllCanonicalNames() {
    return getAllNames().stream()
        .map(FunctionRegistry::canonicalize)
        .collect(Collectors.toSet());
  }

  /// A description of the UDF, which should be used in documentation or for debugging purposes.
  ///
  /// The description should be a human-readable text in markdown format that explains what the function does,
  // language=markdown
  public abstract String getDescription();

  /// Returns the text that should be used in a SQL query to call the function.
  ///
  /// This is used to generate the SQL call for the function in test cases or documentation.
  ///
  /// @param name the name to be used. It should be one of the names returned by getAllFunctionNames().
  /// @param sqlArgValues the values of the arguments to be used in the SQL call. They can be either field references or
  /// literal values, depending on the test case.
  public String asSqlCall(String name, List<String> sqlArgValues) {
    return name + "(" + String.join(", ", sqlArgValues) + ")";
  }

  /// Returns the examples for this Udf.
  ///
  /// As UDFs can have multiple signatures, the examples are grouped by them.
  ///
  /// It is recommended to use [UdfExampleBuilder] in order to build the examples for the Udf.
  public abstract Map<UdfSignature, Set<UdfExample>> getExamples();

  /// The pair of function type and transform function that implements this UDF.
  ///
  /// Unstable API: Transform functions still use an old model of registration using a model that is not polymorphic
  @Nullable
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return null;
  }

  /// Returns the ScalarFunctions that implement this UDF, if any.
  ///
  /// Ideally all UDFs should have a corresponding ScalarFunction. Otherwise Pinot won't be able to use them in some
  /// scenarios. This should be enforced for all new UDFs, but existing UDFs might not have a corresponding
  /// ScalarFunction, in which case they can return null.
  @Nullable
  public abstract PinotScalarFunction getScalarFunction();

  @Override
  public String toString() {
    return getMainName();
  }

  public static abstract class FromAnnotatedMethod extends Udf {
    private final Method _method;
    private final ScalarFunction _annotation;

    public FromAnnotatedMethod(Method method) {
      _method = method;
      _annotation = Preconditions.checkNotNull(method.getAnnotation(ScalarFunction.class),
          "Method %s is not annotated with @ScalarFunction", method);
    }

    @Override
    public Set<String> getAllNames() {
      if (_annotation.names().length > 0) {
        return Set.of(_annotation.names());
      }
      return Set.of(_method.getName());
    }

    @Override
    public String getMainName() {
      if (_annotation.names().length > 0) {
        return _annotation.names()[0];
      }
      return _method.getName();
    }

    @Override
    public PinotScalarFunction getScalarFunction() {
      return PinotScalarFunction.fromMethod(_method, _annotation.isVarArg(),
          _annotation.nullableParameters(), _annotation.names());
    }
  }
}
