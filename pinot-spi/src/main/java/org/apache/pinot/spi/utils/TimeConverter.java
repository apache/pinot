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
package org.apache.pinot.spi.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.TimeGranularitySpec;


/**
 * @deprecated This conversion should be done via transform functions set on the schema field spec
 * TimeConverter to convert value to/from milliseconds since epoch based on the given {@link TimeGranularitySpec}.
 */
public class TimeConverter {
  private final TimeGranularitySpec _timeGranularitySpec;

  public TimeConverter(TimeGranularitySpec timeGranularitySpec) {
    Preconditions.checkArgument(timeGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString()),
        "Cannot perform time conversion for time format other than EPOCH");
    _timeGranularitySpec = timeGranularitySpec;
  }

  public long toMillisSinceEpoch(Object value) {
    long duration;
    if (value instanceof Number) {
      duration = ((Number) value).longValue();
    } else {
      duration = Long.parseLong(value.toString());
    }
    return _timeGranularitySpec.getTimeType().toMillis(duration * _timeGranularitySpec.getTimeUnitSize());
  }

  public Object fromMillisSinceEpoch(long value) {
    long duration = _timeGranularitySpec.getTimeType().convert(value, TimeUnit.MILLISECONDS) / _timeGranularitySpec.getTimeUnitSize();
    switch (_timeGranularitySpec.getDataType()) {
      case INT:
        return (int) duration;
      case LONG:
        return duration;
      case FLOAT:
        return (float) duration;
      case DOUBLE:
        return (double) duration;
      case STRING:
        return Long.toString(duration);
      default:
        throw new IllegalStateException();
    }
  }
}
