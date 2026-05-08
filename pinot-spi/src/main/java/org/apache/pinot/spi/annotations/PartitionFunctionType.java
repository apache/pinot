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


/// Optional annotation declaring registration aliases for plug-in `PartitionFunction`
/// implementations.
///
/// `PartitionFunctionFactory` discovers every public, concrete `PartitionFunction` subtype on the
/// classpath under the `org.apache.pinot.*` package tree. Annotating a class with this annotation
/// is optional and only needed when:
///
/// - The class should be reachable under multiple aliases (e.g. `Murmur` and `Murmur2` for the
///   same impl), or
/// - The annotation-declared name should differ from `PartitionFunction.getName()`.
///
/// When the annotation is absent (or [#names()] is empty), the registry instantiates the class
/// with `(numPartitions=1, functionConfig=null)` and registers under the value returned by
/// `PartitionFunction.getName()`.
///
/// Each registrable class must be public, concrete, live under the `org.apache.pinot` package
/// tree, and expose a public constructor with signature
/// `(int numPartitions, java.util.Map<String, String> functionConfig)`. Implementations that
/// ignore `functionConfig` should accept and discard it.
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PartitionFunctionType {

  /// Canonical name(s) under which to register this partition function. When empty (the default),
  /// the registry probes `PartitionFunction.getName()` instead.
  String[] names() default {};

  /// Set to `false` to skip auto-registration without removing the class.
  boolean enabled() default true;
}
