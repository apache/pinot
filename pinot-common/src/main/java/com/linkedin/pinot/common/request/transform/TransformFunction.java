/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.request.transform;

import com.linkedin.pinot.common.request.transform.result.TransformResult;


/**
 * Interface for TransformFunction functions.
 * The transform function takes list of input values and applies a specific
 * transformation to generate a list of output values.
 */
public interface TransformFunction {

  /**
   * The transform function takes an array of double values
   * and returns a new array of double values.
   *
   * All input double[] are assumed to be of same length, and checks for the
   * same can be skipped for performance reasons.
   *
   *
   * @param length Length of doc ids to process
   * @param input Array of input values
   * @return TransformResult object containing transformed values.
   */
  TransformResult transform(int length, Object... input);

  /**
   * This method returns the name of the transform function.
   *
   * @return Name of the transform function
   */
  String getName();
}
