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
package org.apache.pinot.spi.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation Class for Scalar Functions.
 *
 * Methods annotated using the interface are registered in the FunctionsRegistry, and can be used for transform and
 * filtering during record ingestion, and transform and post-aggregation during query execution.
 *
 * NOTE:
 *   1. The annotated method must be under the package of name 'org.apache.pinot.*.function.*' to be auto-registered.
 *   2. The following parameter types are supported for auto type conversion:
 *     - int/Integer
 *     - long/Long
 *     - float/Float
 *     - double/Double
 *     - String
 *     - byte[]
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ScalarFunction {

  boolean enabled() default true;

  /**
   * If empty, FunctionsRegistry registers the method name as function name;
   * If not empty, FunctionsRegistry only registers the function names specified here, the method name is ignored.
   */
  String[] names() default {};

  /**
   * Whether the scalar function expects and can handle null arguments.
   *
   */
  boolean nullableParameters() default false;

  boolean isPlaceholder() default false;

  /**
   * Whether the scalar function takes various number of arguments.
   */
  boolean isVarArg() default false;
}
