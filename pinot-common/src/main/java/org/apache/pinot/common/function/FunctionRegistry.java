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
import com.google.common.collect.Iterables;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NameMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  private static final FunctionsSchema FUNCTIONS_SCHEMA;
  private static final SqlOperatorTable FUNCTIONS_OP_TABLE;

  /*
   * Registers the scalar functions via reflection.
   * NOTE: In order to plugin methods using reflection, the methods should be inside a class that includes ".function."
   *       in its class path. This convention can significantly reduce the time of class scanning.
   */
  static {
    NameMultimap<Function> functions = new NameMultimap<>();

    long startTimeMs = System.currentTimeMillis();
    Set<Method> methods = PinotReflectionUtils.getMethodsThroughReflection(".*\\.function\\..*", ScalarFunction.class);
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction.enabled()) {
        // Annotated function names
        String[] scalarFunctionNames = scalarFunction.names();
        if (scalarFunctionNames.length == 0) {
          registerFunction(functions, method);
        }

        for (String name : scalarFunctionNames) {
          FunctionRegistry.registerFunction(functions, name, method);
        }
      }
    }
    FUNCTIONS_SCHEMA = new FunctionsSchema(functions);

    Properties catalogReaderConfigProperties = new Properties();
    catalogReaderConfigProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");

    CalciteSchema schema = CalciteSchema.createRootSchema(false, true, "functionSchema", FUNCTIONS_SCHEMA);
    FUNCTIONS_OP_TABLE = new CalciteCatalogReader(
        schema,
        ImmutableList.of(),
        new JavaTypeFactoryImpl(),
        new CalciteConnectionConfigImpl(catalogReaderConfigProperties));

    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", functions.map().size(),
        functions.map().keySet(), System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  public static void registerFunction(NameMultimap<Function> functions, Method method) {
    registerFunction(functions, method.getName(), method);
  }

  /**
   * Registers a method with the name of the method.
   */
  public static void registerFunction(NameMultimap<Function> functions, String name, Method method) {
    if (method.getAnnotation(Deprecated.class) == null) {
      Function function = ScalarFunctionImpl.create(method);
      functions.put(name, function);
      if (!canonicalize(name).equals(name)) {
        // this is for backwards compatibility with V1 engine, which
        // always looks up case-insensitive names for functions but
        // case sensitive names for other identifiers (calcite does
        // not have an option to do that, it is either entirely case
        // sensitive or insensitive)
        functions.put(canonicalize(name), function);
      }
    }
  }

  public static Map<String, Collection<Function>> getRegisteredCalciteFunctionMap() {
    return FUNCTIONS_SCHEMA.getFunctionNames().stream().collect(Collectors.toMap(
        name -> name, FUNCTIONS_SCHEMA::getFunctions
    ));
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return FUNCTIONS_SCHEMA.getFunctionNames().contains(canonicalize(functionName));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  public static FunctionInfo getFunctionInfo(String functionName, List<RelDataType> parameters) {
    SqlIdentifier functionIdentifier = new SqlIdentifier(functionName, SqlParserPos.ZERO);
    List<SqlUserDefinedFunction> functionList = getFunctionInfo(functionIdentifier, parameters, false);

    if (functionList.isEmpty()) {
      // couldn't find anything without coercion, try coercing
      functionList = getFunctionInfo(functionIdentifier, parameters, true);
    }

    if (functionList.isEmpty()) {
      return null;
    } else if (functionList.size() > 1) {
      throw new BadQueryRequestException(
          String.format("Multiple functions match the desired name(%s)/parameter(%s) pairing): %s",
              functionName, parameters, functionList));
    } else {
      // the only type of functions we register here are scalar functions, which means
      // all operators will be of type SqlUserDefinedFunction. we may need to make this
      // more robust if we support UDAFs
      SqlUserDefinedFunction fun = Iterables.getOnlyElement(functionList);
      return new FunctionInfo(fun.getFunction());
    }
  }

  private static List<SqlUserDefinedFunction> getFunctionInfo(SqlIdentifier functionName, List<RelDataType> parameters,
      boolean coerce) {
    Iterator<SqlOperator> functions =
        SqlUtil.lookupSubjectRoutines(FUNCTIONS_OP_TABLE, new JavaTypeFactoryImpl(), functionName, parameters, null,
            SqlSyntax.FUNCTION, SqlKind.ALL, null, SqlNameMatchers.withCaseSensitive(true), coerce);

    ArrayList<SqlUserDefinedFunction> functionList = new ArrayList<>();
    while (functions.hasNext()) {
      SqlOperator op = functions.next();
      if (!(op instanceof SqlUserDefinedFunction)) {
        // this shouldn't happen, so we'll just log a warning and continue in case
        // we find a sql function that does match
        LOGGER.warn("Unexpected operator type {} returned when searching for {}", op.getClass(), functionName);
      } else {
        functionList.add(((SqlUserDefinedFunction) op));
      }
    }

    return functionList;
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }
}
