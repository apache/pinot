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

package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;


public class TimeSeriesDataPoint {

  private String metricUrn;
  private long timestamp;
  private long metricId;
  private String dataValue;

  public TimeSeriesDataPoint(String metricUrn, long timestamp, long metricId, String dataValue) {
    this.metricUrn = metricUrn;
    this.timestamp = timestamp;
    this.metricId = metricId;
    this.dataValue = dataValue;
  }

  public String getMetricUrn() { return metricUrn; }
  public long getTimestamp() { return timestamp; }
  public long getMetricId() { return metricId; }
  public String getDataValue() { return dataValue; }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public void setDataValue(String dataValue) {
    this.dataValue = dataValue;
  }

  public String getDocumentKey() {
    return metricId + "_" + timestamp;
  }

  public String getMetricUrnHash() {
    return CacheUtils.hashMetricUrn(metricUrn);
  }

}
