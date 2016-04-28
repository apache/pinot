/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;

public class TimeConverterProvider {

  private static final String TO_SECONDS_TIME_SPEC_NAME = "toSeconds";

  public static TimeConverter getTimeConverterFromGranularitySpecs(Schema schema) {
    if (schema.getTimeFieldSpec().getOutgoingGranularitySpec() == null ||
        schema.getTimeFieldSpec().getIncomingGranularitySpec().equals(
            schema.getTimeFieldSpec().getOutgoingGranularitySpec())) {
      return new NoOpTimeConverter(schema.getTimeFieldSpec().getIncomingGranularitySpec());
    } else {
      return new GeneralTimeConverter(schema.getTimeFieldSpec().getIncomingGranularitySpec(),
          schema.getTimeFieldSpec().getOutgoingGranularitySpec());
    }
  }

  public static TimeConverter getToSecondsTimeConverterFromGranularitySpecs(Schema schema) {
    return new GeneralTimeConverter(schema.getTimeFieldSpec().getIncomingGranularitySpec(),
        new TimeGranularitySpec(DataType.INT, TimeUnit.SECONDS, TO_SECONDS_TIME_SPEC_NAME));
  }

}
