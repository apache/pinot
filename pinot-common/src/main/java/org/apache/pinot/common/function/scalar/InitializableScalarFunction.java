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
package org.apache.pinot.common.function.scalar;

/**
 * Interface that defines {@link org.apache.pinot.spi.annotations.ScalarFunction}
 * annotated Function that supports preprocess literal arguments.
 */
public interface InitializableScalarFunction {

  /**
   * Initialize internal state. This will be called by the query expression parser
   * during expression parsing and called with argument.
   *
   * Each concrete implementation should check and validate the desired positional
   * arguments and ensure that it is of literal type.
   *
   * @param arguments positional argument expressions.
   */
  void init(Object... arguments);
}
