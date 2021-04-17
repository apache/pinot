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
package org.apache.pinot.core.operator.transform.transformer.timeunit;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;


/**
 * Implementation of {@link TimeUnitTransformer} to handle custom time units such as WEEKS, MONTHS, YEARS.
 */
public class CustomTimeUnitTransformer implements TimeUnitTransformer {
  private TimeUnit _inputTimeUnit;
  private CustomTimeUnit _outputTimeUnit;

  private enum CustomTimeUnit {
    WEEKS {
      @Override
      long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.weeks().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    },
    MONTHS {
      @Override
      long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.months().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    },
    YEARS {
      @Override
      long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.years().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    };

    /**
     * Convert the given millisecond since epoch into the desired time unit.
     *
     * @param millisSinceEpoch Millisecond since epoch
     * @return Time since epoch of desired time unit
     */
    abstract long fromMillis(long millisSinceEpoch);
  }

  public CustomTimeUnitTransformer(@Nonnull TimeUnit inputTimeUnit, @Nonnull String outputTimeUnitName) {
    _inputTimeUnit = inputTimeUnit;
    _outputTimeUnit = CustomTimeUnit.valueOf(outputTimeUnitName);
  }

  @Override
  public void transform(@Nonnull long[] input, @Nonnull long[] output, int length) {
    for (int i = 0; i < length; i++) {
      output[i] = _outputTimeUnit.fromMillis(_inputTimeUnit.toMillis(input[i]));
    }
  }
}
