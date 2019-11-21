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

import org.apache.pinot.thirdeye.util.CacheUtils;


/**
 * Class used to represent a single "data point". It contains the timestamp associated with the
 * data point and also the value, plus the associated metricUrn and metricId as metadata.
 */
public class TimeSeriesDataPoint {

  private String metricUrn;
  private long timestamp;
  private long metricId;
  private String dataValue;
  private String metricUrnHash;

  public TimeSeriesDataPoint(String metricUrn, long timestamp, long metricId, String dataValue) {
    this.metricUrn = metricUrn;
    this.timestamp = timestamp;
    this.metricId = metricId;
    this.dataValue = dataValue;
    this.metricUrnHash = CacheUtils.hashMetricUrn(metricUrn);
  }

  public String getMetricUrn() { return metricUrn; }
  public long getTimestamp() { return timestamp; }
  public long getMetricId() { return metricId; }

  /**
   * Different data sources may use different representations for missing data.
   * For example, Pinot datasets may use null values to represent missing data,
   * but Presto might use 0 instead. To be consistent, we will use 0's to represent
   * the missing data values.
   * @return either the data value or "0".
   */
  public String getDataValue() {
    if (dataValue == null || dataValue.toLowerCase().equals("null")) {
      return "0";
    }

    return dataValue;
  }

  /**
   * gets data value as a double for storing into cache. We don't want to
   * directly use something like Double.parseDouble(dataValue) in other parts
   * of the code because handling the possible null values would be messy.
   * @return data value as double, or 0 if it is null
   */
  public double getDataValueAsDouble() {
    return Double.parseDouble(this.getDataValue());
  }

  /**
   * We use this the hashed metricURN (using CRC32) as the key for the
   * associated key-value pair in Couchbase.
   * @return hashed metricURN
   */
  public String getMetricUrnHash() { return metricUrnHash; }

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

  /**
   * We use the appended metricId and timestamp together
   * as the document key for the data point in the cache.
   * This might look something like:
   * 1351840_1185783260000
   * @return document key
   */
  public String getDocumentKey() {
    return metricId + "_" + timestamp;
  }
}
