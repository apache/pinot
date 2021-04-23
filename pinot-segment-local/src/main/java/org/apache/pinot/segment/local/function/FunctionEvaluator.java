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
package org.apache.pinot.segment.local.function;

import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Interface for evaluators of transform function expressions of schema field specs
 */
public interface FunctionEvaluator {

  /**
   * Get the arguments of the function
   */
  List<String> getArguments();

  /**
   * Evaluate the function on the generic row and return the result
   */
  Object evaluate(GenericRow genericRow);

  /**
   * Evaluates the function on the given values (same order as the arguments) and returns the result.
   */
  Object evaluate(Object[] values);
}
