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

public class GeneralTimeConverter implements TimeConverter {

  TimeGranularitySpec incoming;
  TimeGranularitySpec outgoing;

  public GeneralTimeConverter(TimeGranularitySpec incoming, TimeGranularitySpec outgoing) {
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  @Override
  public Object convert(Object incomingTime) {
    return toValueFromDateTime(getDateTimeFrom(incomingTime));
  }

  @Override
  public DateTime getDateTimeFrom(Object o) {
    if (o == null) {
      return new DateTime(0);
    }
    long incoming;
    if (o instanceof Number) {
      incoming = ((Number) o).longValue();
    } else {
      incoming = Long.valueOf(o.toString());
    }
    return new DateTime(this.incoming.getTimeType().toMillis(incoming));
  }

  @Override
  public Object toValueFromDateTime(DateTime dt) {
    return outgoing.getTimeType().convert(dt.getMillis(), TimeUnit.MILLISECONDS);
  }
}
