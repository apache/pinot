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
package org.apache.pinot.segment.spi.function;

import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Evaluates a function-style expression against Pinot row data or positional argument values.
 */
public interface FunctionEvaluator {

  /**
   * Returns the argument names referenced by the evaluator in positional order.
   */
  List<String> getArguments();

  /**
   * Evaluates the function on the given {@link GenericRow}.
   */
  Object evaluate(GenericRow genericRow);

  /**
   * Evaluates the function on the given values in the same order as {@link #getArguments()}.
   */
  Object evaluate(Object[] values);
}
