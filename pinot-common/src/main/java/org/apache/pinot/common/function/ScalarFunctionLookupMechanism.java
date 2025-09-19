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

import java.util.Set;


/**
 * Interface for looking up scalar functions.
 *
 * Each instance of this interface represents a mechanism to look up scalar functions. They should be registered
 * as a service provider in the META-INF/services directory to be discovered by the ServiceLoader.
 * Alternatively, they can be registered using the {@link com.google.auto.service.AutoService} annotation.
 *
 * These services will rarely be used, usually only once at startup.
 *
 * @see AnnotatedClassLookupMechanism
 * @see AnnotatedMethodLookupMechanism
 */
public interface ScalarFunctionLookupMechanism {

  /**
   * Returns the set of {@link ScalarFunctionProvider} instances that can be used to look up scalar functions.
   */
  Set<ScalarFunctionProvider> getProviders();

  /**
   * Interface for providing scalar functions.
   * <p>Each provider can provide multiple scalar functions, all with the same priority.
   * <p>If two functions have the same canonical name (which means they are found by different providers),
   * the one with higher priority will be used.
   */
  interface ScalarFunctionProvider {
    /**
     * Returns the name of the provider, not the functions it provides.
     *
     * <p>This is used for logging and debugging purposes when there are multiple providers for the same function.
     */
    String name();

    /**
     * Returns the priority of the provider. In case two functions have the same canonical name,
     * the one with higher priority will be used.
     *
     * <p>Default priority is 0.
     */
    default int priority() {
      return 0;
    }

    /**
     * Returns a set of {@link PinotScalarFunction} instances.
     */
    Set<PinotScalarFunction> getFunctions();
  }
}
