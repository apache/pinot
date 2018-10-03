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
package com.linkedin.thirdeye.hadoop.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;

import java.util.concurrent.TimeUnit;

/** This class represents the time spec for thirdeye-hadoop jobs
 * @param columnName - columnName which represents time
 * @param timeGranularity - time granularity for the time column
 */
public class TimeSpec {
  private static final TimeGranularity DEFAULT_TIME_GRANULARITY = new TimeGranularity(1, TimeUnit.HOURS);
  private static final String DEFAULT_TIME_FORMAT = TimeFormat.EPOCH.toString();

  private String columnName;
  private TimeGranularity timeGranularity = DEFAULT_TIME_GRANULARITY;
  private String timeFormat = DEFAULT_TIME_FORMAT;

  public TimeSpec() {
  }

  public TimeSpec(String columnName, TimeGranularity timeGranularity, String timeFormat) {
    this.columnName = columnName;
    this.timeGranularity = timeGranularity;
    this.timeFormat = timeFormat;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public TimeGranularity getTimeGranularity() {
    return timeGranularity;
  }

  @JsonProperty
  public String getTimeFormat() {
    return timeFormat;
  }


}
