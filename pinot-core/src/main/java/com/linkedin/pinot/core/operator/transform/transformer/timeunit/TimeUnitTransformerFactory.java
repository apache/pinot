/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.transform.transformer.timeunit;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


public class TimeUnitTransformerFactory {
  private TimeUnitTransformerFactory() {
  }

  public static TimeUnitTransformer getTimeUnitTransformer(@Nonnull TimeUnit inputTimeUnit,
      @Nonnull String outputTimeUnitName) {
    outputTimeUnitName = outputTimeUnitName.toUpperCase();
    try {
      return new JavaTimeUnitTransformer(inputTimeUnit, TimeUnit.valueOf(outputTimeUnitName));
    } catch (Exception e) {
      return new CustomTimeUnitTransformer(inputTimeUnit, outputTimeUnitName);
    }
  }
}
