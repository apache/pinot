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
package com.linkedin.pinot.core.operator.transform.function.time.converter;

import java.util.concurrent.TimeUnit;


/**
 * Interface for converting from {@link TimeUnit} into other time units either in {@link TimeUnit}
 * or custom defined.
 */
public interface TimeUnitConverter {

  /**
   * This method converts an array of input times from the specified {@link TimeUnit} into
   * implementation's timeUnit.
   *
   * @param inputTime Input times
   * @param inputTimeUnit Time unit for the input
   * @param length Length of input array to process
   * @param outputTime Array where output is to be stored.
   */
  void convert(long[] inputTime, TimeUnit inputTimeUnit, int length, long[] outputTime);
}
