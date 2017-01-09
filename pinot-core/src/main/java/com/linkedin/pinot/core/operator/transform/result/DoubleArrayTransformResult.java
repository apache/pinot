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
package com.linkedin.pinot.core.operator.transform.result;

import com.linkedin.pinot.common.request.transform.result.TransformResult;


/**
 * This implementation of {@link TransformResult} uses primitive double array
 * as underlying storage for holding the transform results.
 */
public class DoubleArrayTransformResult implements TransformResult {

  private final double[] _values;

  /**
   * Constructor for the class.
   *
   * @param values Result values to store.
   */
  public DoubleArrayTransformResult(double[] values) {
    this._values = values;
  }

  /**
   * {@inheritDoc}
   *
   * @param <T> Data type for the result
   * @return Result array
   */
  @Override
  public <T> T getResultArray() {
    return (T) _values;
  }
}
