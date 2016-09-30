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
package com.linkedin.pinot.common.utils.time;

import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;

public class DefaultTimeConverter implements TimeConverter {

  TimeGranularitySpec incoming;
  TimeGranularitySpec outgoing;
  private boolean conversionSupported;
  private boolean needConversion;

  public void init(TimeGranularitySpec incoming, TimeGranularitySpec outgoing) {
    this.incoming = incoming;
    this.outgoing = outgoing;
    conversionSupported = false;
    needConversion = true;
    if(incoming.equals(outgoing)){
      needConversion = false;
    }
    if (TimeFormat.EPOCH.toString().equals(incoming.getTimeFormat())
        && TimeFormat.EPOCH.toString().equals(outgoing.getTimeFormat())) {
      conversionSupported = true;
    }
    if (needConversion && !conversionSupported) {
      //TODO: Handle conversion between sdf <-> epoch
      throw new RuntimeException(
          "Conversion from Simple Date Format to epoch/simpleDateFormat is not supported");
    }
  }

  @Override
  public Object convert(Object incomingTimeValue) {
    if (incomingTimeValue == null) {
      return null;
    }
    long duration;
    if (incomingTimeValue instanceof Number) {
      duration = ((Number) incomingTimeValue).longValue();
    } else {
      duration = Long.parseLong(incomingTimeValue.toString());
    }
    if (conversionSupported) {
      long outgoingTime = outgoing.getTimeType().convert(duration * incoming.getTimeUnitSize(),
          incoming.getTimeType());
      return convertToOutgoingDataType(outgoingTime / outgoing.getTimeUnitSize());
    } else {
      //TODO: Handle conversion between sdf <-> epoch
      throw new RuntimeException(
          "Conversion from Simple Date Format to epoch/simpleDateFormat is not supported");
    }
  }

  private Object convertToOutgoingDataType(long outgoingTimeValue) {
    switch (outgoing.getDataType()) {
    case LONG:
      return outgoingTimeValue;
    case STRING:
      return new Long(outgoingTimeValue).toString();
    case INT:
      return new Long(outgoingTimeValue).intValue();
    case SHORT:
      return new Long(outgoingTimeValue).shortValue();
    case FLOAT:
      return new Long(outgoingTimeValue).floatValue();
    case DOUBLE:
      return new Long(outgoingTimeValue).doubleValue();
    default:
      return outgoingTimeValue;
    }
  }
}
