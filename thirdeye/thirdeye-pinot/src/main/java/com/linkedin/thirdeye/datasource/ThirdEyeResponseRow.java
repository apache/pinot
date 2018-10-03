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

package com.linkedin.thirdeye.datasource;

import java.util.List;

public class ThirdEyeResponseRow {

  int timeBucketId;
  List<String> dimensions;
  List<Double> metrics;

  public ThirdEyeResponseRow(int timeBucketId, List<String> dimensions, List<Double> metrics) {
    super();
    this.timeBucketId = timeBucketId;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public List<Double> getMetrics() {
    return metrics;
  }
  
  public int getTimeBucketId() {
    return timeBucketId;
  }

}
