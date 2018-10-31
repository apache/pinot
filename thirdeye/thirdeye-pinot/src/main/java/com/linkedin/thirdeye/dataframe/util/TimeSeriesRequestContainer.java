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

package com.linkedin.thirdeye.dataframe.util;

import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.Period;


public class TimeSeriesRequestContainer extends RequestContainer {
  final DateTime start;
  final DateTime end;
  final Period interval;

  public TimeSeriesRequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions, DateTime start,
      DateTime end, Period interval) {
    super(request, expressions);
    this.start = start;
    this.end = end;
    this.interval = interval;
  }

  public DateTime getStart() {
    return start;
  }

  public DateTime getEnd() {
    return end;
  }

  public Period getInterval() {
    return interval;
  }
}
