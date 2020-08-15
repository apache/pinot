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

package org.apache.pinot.thirdeye.anomaly.views;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.Minutes;


/**
 * We face a problem that if we store the timelines view using AnomalyTimelinesView. The DB has overflow exception when
 * storing merged anomalies. As TimeBucket is not space efficient for storage. To solve the problem, we introduce a
 * condensed version of AnomalyTimelinesView.
 * To save the space in DB, three techniques has been applied:
 *  - Flatten the time buckets to timestamp and bucket millis
 *  - An offset has been saved; no need to store the whole timestamp value
 *  - A bucket unit, 1_MIN, is applied to reduce the space (Currently we don't have time granularity less than 1_MIN)
 * An compress method is also implemented. It compresses the time series data by enlarge the bucket millis.
 */
public class CondensedAnomalyTimelinesView {
  public static final int DEFAULT_MAX_LENGTH = 1024 * 10; // 10 kilobytes
  public static final int DEFAULT_DECIMAL_DIGITS = 3; // only keep 3 decimal digits
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final Long DEFAULT_MIN_BUCKET_UNIT = Minutes.ONE.toStandardDuration().getMillis();

  Long timestampOffset = 0l; // the timestamp offset of the original time series
  Long minBucketUnit = 1l;
  Long bucketMillis = 0l;  // the bucket size of original time series
  List<Long> timeStamps = new ArrayList<>(); // the start time of data points
  Map<String, String> summary = new HashMap<>(); // the summary of the view
  List<Double> currentValues = new ArrayList<>();  // the observed values
  List<Double> baselineValues = new ArrayList<>(); // the expected values

  public CondensedAnomalyTimelinesView() {

  }

  public CondensedAnomalyTimelinesView(Long bucketMillis, List<Long> timeStamps, List<Double> currentValues,
      List<Double> baselineValues, Map<String, String> summary) {
    this.timestampOffset = timeStamps.get(0);
    this.minBucketUnit = DEFAULT_MIN_BUCKET_UNIT;
    this.bucketMillis = bucketMillis / minBucketUnit;
    for (long timestamp : timeStamps) {
      this.timeStamps.add((timestamp - timestampOffset)/minBucketUnit);
    }
    this.summary = summary;
    this.currentValues = currentValues;
    this.baselineValues = baselineValues;
  }

  public Long getBucketMillis() {
    return bucketMillis;
  }

  public void setBucketMillis(Long bucketMillis) {
    this.bucketMillis = bucketMillis;
  }

  public List<Long> getTimeStamps() {
    return this.timeStamps;
  }

  public void addRealTimeStamps(Long timeStamp) {
    this.timeStamps.add((timeStamp - timestampOffset) / DEFAULT_MIN_BUCKET_UNIT);
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void addSummary(String key, String value) {
    this.summary.put(key, value);
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void addCurrentValues(Double currentValue) {
    this.currentValues.add(currentValue);
  }

  public List<Double> getBaselineValues() {
    return baselineValues;
  }

  public void addBaselineValues(Double baselineValue) {
    this.baselineValues.add(baselineValue);
  }

  public Long getTimestampOffset() {
    return timestampOffset;
  }

  public void setTimestampOffset(Long timestampOffset) {
    this.timestampOffset = timestampOffset;
  }

  public Long getMinBucketUnit() {
    return minBucketUnit;
  }

  public void setMinBucketUnit(Long minBucketUnit) {
    this.minBucketUnit = minBucketUnit;
  }

  /**
   * Convert current instance to an AnomalyTimelinesView instance
   * @return
   */
  public AnomalyTimelinesView toAnomalyTimelinesView() {
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();
    for (int i = 0; i < timeStamps.size(); i++) {
      long startTime = timeStamps.get(i);
      TimeBucket timeBucket = new TimeBucket(startTime * minBucketUnit + timestampOffset,
          (startTime + bucketMillis) * minBucketUnit + timestampOffset,
          startTime * minBucketUnit + timestampOffset,
          (startTime + bucketMillis) * minBucketUnit + timestampOffset);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      if (i < baselineValues.size()) {
        anomalyTimelinesView.addBaselineValues(baselineValues.get(i));
      } else {
        anomalyTimelinesView.addCurrentValues(Double.NaN);
      }
      if (i < currentValues.size()) { // In case there is no current value in view
        anomalyTimelinesView.addCurrentValues(currentValues.get(i));
      } else {
        anomalyTimelinesView.addCurrentValues(Double.NaN);
      }
    }
    for (Map.Entry<String, String> entry : summary.entrySet()) {
      anomalyTimelinesView.addSummary(entry.getKey(), entry.getValue());
    }
    return anomalyTimelinesView;
  }

  /**
   * Convert an AnomalyTimelinesView instance to CondensedAnomalyTimelinesView
   * @param anomalyTimelinesView
   * @return
   */
  public static CondensedAnomalyTimelinesView fromAnomalyTimelinesView(AnomalyTimelinesView anomalyTimelinesView) {
    if (anomalyTimelinesView == null || anomalyTimelinesView.getTimeBuckets().isEmpty()) {
      return new CondensedAnomalyTimelinesView();
    }
    long maxBucketMillis = 0l;
    List<Long> timestamps = new ArrayList<>();
    for (int i = 0; i < anomalyTimelinesView.getTimeBuckets().size(); i++) {
      TimeBucket timeBucket = anomalyTimelinesView.getTimeBuckets().get(i);
      maxBucketMillis = Math.max(maxBucketMillis, timeBucket.getCurrentEnd() - timeBucket.getCurrentStart());
      timestamps.add(timeBucket.getCurrentStart());
    }
    return new CondensedAnomalyTimelinesView(maxBucketMillis, timestamps, anomalyTimelinesView.getCurrentValues(),
        anomalyTimelinesView.getBaselineValues(), anomalyTimelinesView.getSummary());
  }

  /**
   * Compress this instance of timelines view to under DEFAULT_MAX_LENGTH bytes
   * The concept of the time series compression is to enlarge the bucket size; from 5 min to 10 min for example.
   * @return a compressed CondensedAnomalyTimelinesView
   */
  public CondensedAnomalyTimelinesView compress() {
    if (timeStamps.size() == 0) {
      return this;
    }
    try {
      if (this.toJsonString().length() > DEFAULT_MAX_LENGTH) {
        // First try rounding up
        roundUp();
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to parse view to json string", e);
    }
    return compress(DEFAULT_MAX_LENGTH);
  }

  private void roundUp() {
    List<Double> roundedObservedValues = new ArrayList<>();
    List<Double> roundedExpectedValues = new ArrayList<>();
    for (int i = 0; i < timeStamps.size(); i++) {
      Double roundedObservedValue = Math.round(currentValues.get(i) * (Math.pow(10, DEFAULT_DECIMAL_DIGITS))) / (Math.pow(10, DEFAULT_DECIMAL_DIGITS));
      Double roundedExpectedValue = Math.round(baselineValues.get(i) * (Math.pow(10, DEFAULT_DECIMAL_DIGITS))) / (Math.pow(10, DEFAULT_DECIMAL_DIGITS));
      roundedObservedValues.add(roundedObservedValue);
      roundedExpectedValues.add(roundedExpectedValue);
    }
    this.currentValues = roundedObservedValues;
    this.baselineValues = roundedExpectedValues;
  }

  /**
   * Compress this instance of timelines view to under ${maxLength} bytes
   * The concept of the time series compression is to enlarge the bucket size; from 5 min to 10 min for example.
   * @param maxLength customized maximum length
   * @return a compressed CondensedAnomalyTimelinesView
   */
  public CondensedAnomalyTimelinesView compress(int maxLength) {
    if (timeStamps.size() == 0) {
      return this;
    }
    try {
      if (this.toJsonString().length() < maxLength) {
        return this;
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to parse view to json string", e);
    }

    List<Long> aggregatedTimestamps = new ArrayList<>();
    List<Double> aggregatedObservedValues = new ArrayList<>();
    List<Double> aggregatedExpectedValues = new ArrayList<>();
    long maxBucketMills = this.bucketMillis * 2;
    long lastTimestampEnd = this.timeStamps.get(this.timeStamps.size() - 1) + this.bucketMillis;

    for (int i = 0; i < timeStamps.size(); i++) {
      int count = 1;
      long timestamp = this.timeStamps.get(i);
      double observedValue = this.currentValues.get(i);
      double expectedValue = this.baselineValues.get(i);
      /*
       Aggregate data points fit in the bucket window; remain the same if it cannot fit
       */
      if ((lastTimestampEnd - timestamp) >= maxBucketMills) {
        while (i + 1 < this.timeStamps.size() && (this.timeStamps.get(i + 1) - timestamp) < maxBucketMills) {
          observedValue += this.currentValues.get(i + 1);
          expectedValue += this.baselineValues.get(i + 1);
          i++;
          count++;
        }
      }
      aggregatedTimestamps.add(timestamp * DEFAULT_MIN_BUCKET_UNIT + timestampOffset);
      aggregatedObservedValues.add(observedValue/((double)count));
      aggregatedExpectedValues.add(expectedValue/((double)count));
    }


    return new CondensedAnomalyTimelinesView(maxBucketMills * DEFAULT_MIN_BUCKET_UNIT, aggregatedTimestamps, aggregatedObservedValues,
        aggregatedExpectedValues, new HashMap<String, String>(summary)).compress(maxLength);
  }

  /**
   * Convert current instance into JSON String using ObjectMapper
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @return
   *    The JSON String of current instance
   * @throws JsonProcessingException
   */
  public String toJsonString() throws JsonProcessingException {
    String jsonString = OBJECT_MAPPER.writeValueAsString(this);

    return jsonString;
  }

  /**
   * Given the JSON String of an AnomalyTimelinesView, return an instance of AnomalyTimelinesView
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @param jsonString
   *    The JSON String of an instance
   * @return
   *    An instance based on the given JSON String
   * @throws IOException
   */
  public static CondensedAnomalyTimelinesView fromJsonString(String jsonString) throws IOException {
    CondensedAnomalyTimelinesView view = OBJECT_MAPPER.readValue(jsonString, CondensedAnomalyTimelinesView.class);
    return view;
  }
}
