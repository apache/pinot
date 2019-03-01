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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;


public class PredictionView {

  public Map<String, double[]> getTimeSeries() {
    return timeSeries;
  }

  public String getMetricUrn() {
    return metricUrn;
  }


  Map<String, double[]> timeSeries;
  private String metricUrn;

  public PredictionView(TimeSeries timeSeries) {
    this.metricUrn = timeSeries.getMetricUrn();
    this.timeSeries = new HashMap<>();
    for (String name : timeSeries.getDf().getSeriesNames()) {
      this.timeSeries.put(name, timeSeries.getDf().getDoubles(name).values());
    }
  }
}
