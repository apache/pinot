/*
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

package org.apache.pinot.thirdeye.common.time;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


public class TimeSpec {
  private static final TimeGranularity DEFAULT_TIME_GRANULARITY= new TimeGranularity(1, TimeUnit.DAYS);
  private String columnName;
  private TimeGranularity dataGranularity = DEFAULT_TIME_GRANULARITY;
  private String format = SINCE_EPOCH_FORMAT; //sinceEpoch or yyyyMMdd
  public static String SINCE_EPOCH_FORMAT  = DateTimeFieldSpec.TimeFormat.EPOCH.toString();
  public static String DEFAULT_TIMEZONE = "UTC";

  public TimeSpec() {
  }

  public TimeSpec(String columnName, TimeGranularity dataGranularity, String format) {
    this.columnName = columnName;
    this.dataGranularity = dataGranularity;
    this.format = format;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public TimeGranularity getDataGranularity() {
    return dataGranularity;
  }

  @JsonProperty
  public String getFormat() {
    return format;
  }
}
