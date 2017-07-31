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
 * Implementation of {@link TimeUnitConverter} for handling time units defined in {@link TimeUnit}.
 */
public class JavaTimeUnitConverter implements TimeUnitConverter {
  private final TimeUnit _timeUnit;

  public JavaTimeUnitConverter(String timeUnitName) {
    _timeUnit = TimeUnit.valueOf(timeUnitName);
  }

  @Override
  public void convert(long[] inputTime, TimeUnit inputTimeUnit, int length, long[] outputTime) {
    for (int i = 0; i < length; i++) {
      outputTime[i] = _timeUnit.convert(inputTime[i], inputTimeUnit);
    }
  }
}
