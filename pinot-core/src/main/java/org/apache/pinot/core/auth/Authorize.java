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

package org.apache.pinot.core.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/// Annotation to be used on top of REST endpoints. Methods annotated with this annotation automatically get
/// authorized in [AuthenticationFilter] and if validation passes, then the methods get executed.
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Authorize {
  // Type of the target resource
  TargetType targetType();

  // The query or path parameter to use to get the id of the resource
  // If the target type is the Pinot cluster, then this field is not required
  String paramName() default "";

  /// Optional path or query parameter that narrows a cluster operation to one table for coarse access control.
  /// Use only when the entire operation is limited to that table, rather than filtering part of the response.
  /// A missing parameter, or a name absent from the endpoint's path parameters and declared query parameters,
  /// resolves no table and retains cluster-wide scope, which denies table-scoped principals. Fine-grained
  /// authorization still uses targetType.
  String tableScopeParam() default "";

  // action to validate on the specific resource
  String action();
}
