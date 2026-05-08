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
 * Marker annotation for plug-in {@code PartitionFunction} implementations.
 *
 * <p>Classes annotated with this annotation are auto-discovered at startup by
 * {@code PartitionFunctionFactory} via classpath scanning. Each annotated class must:
 * <ul>
 *   <li>Implement {@code org.apache.pinot.segment.spi.partition.PartitionFunction}</li>
 *   <li>Be public</li>
 *   <li>Live under a package matching {@code .*\.partition\.function\..*} (e.g.
 *       {@code org.apache.pinot.common.partition.function} or any plugin package
 *       that follows the same convention)</li>
 *   <li>Expose a public constructor with signature
 *       {@code (int numPartitions, java.util.Map<String, String> functionConfig)}.
 *       Implementations that ignore {@code functionConfig} should accept and discard it.</li>
 * </ul>
 *
 * <p>Multiple aliases can be declared in {@link #names()} so a single class can be
 * registered under several names (e.g. {@code Murmur} and {@code Murmur2} both map to
 * {@code MurmurPartitionFunction}). Names are matched case-insensitively.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PartitionFunctionType {

  /**
   * Canonical name(s) under which to register this partition function. Must contain at
   * least one entry. The first entry is treated as the canonical name; remaining entries
   * are aliases.
   */
  String[] names();

  /**
   * Set to {@code false} to skip auto-registration without removing the class.
   */
  boolean enabled() default true;
}
