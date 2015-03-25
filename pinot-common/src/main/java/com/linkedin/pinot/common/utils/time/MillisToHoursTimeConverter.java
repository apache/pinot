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

import org.joda.time.DateTime;

import com.linkedin.pinot.common.data.TimeGranularitySpec;


public class MillisToHoursTimeConverter implements TimeConverter {

  TimeGranularitySpec incoming;
  TimeGranularitySpec outgoing;

  public MillisToHoursTimeConverter(TimeGranularitySpec incoming, TimeGranularitySpec outgoing) {
    incoming = this.incoming;
    outgoing = this.outgoing;
  }

  @Override
  public Object convert(Object incomingTime) {
    long incomingInLong = -1;
    switch (incoming.getDataType()) {
      case INT:
        incomingInLong = new Long((Integer) incomingTime).longValue();
        break;
      case LONG:
        incomingInLong = ((Long) incomingTime).longValue();
    }
    return TimeUnit.MILLISECONDS.toHours(incomingInLong);
  }

  @Override
  public DateTime getDataTimeFrom(Object o) {
    long incoming = -1;
    if (o instanceof Integer) {
      incoming = ((Integer) o).longValue();
    } else {
      incoming = (Long) o;
    }
    return new DateTime(TimeUnit.HOURS.toMillis(incoming));
  }
}
