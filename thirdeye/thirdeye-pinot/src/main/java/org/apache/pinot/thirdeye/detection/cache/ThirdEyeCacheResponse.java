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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class used to represent the response from fetch request to centralized cache.
 */

public class ThirdEyeCacheResponse {

  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeCacheResponse.class);

  private final ThirdEyeCacheRequest request;
  private List<TimeSeriesDataPoint> timeSeriesRows;
  private long firstTimestamp;
  private long lastTimestamp;

  public ThirdEyeCacheResponse(ThirdEyeCacheRequest request, List<TimeSeriesDataPoint> timeSeriesRows) {
    this.request = request;
    this.timeSeriesRows = timeSeriesRows;
    this.initializeFirstAndLastTimestamps();
  }

  /**
   * sets the first and last timestamps from the cache result. If no documents
   * were fetched (we had a complete cache miss), firstTimestamp and lastTimestamp
   * will be initialized to Long.MAX_VALUE and Long.MIN_VALUE respectively so that
   * they don't cause issues with checking for missing data slices.
   */
  private void initializeFirstAndLastTimestamps() {
    if (this.hasNoRows()) {
      this.setFirstTimestamp(Long.MAX_VALUE);
      this.setLastTimestamp(Long.MIN_VALUE);
    } else {
      this.setFirstTimestamp(timeSeriesRows.get(0).getTimestamp());
      this.setLastTimestamp(timeSeriesRows.get(timeSeriesRows.size() - 1).getTimestamp());
    }
  }

  public ThirdEyeCacheRequest getCacheRequest() { return request; }
  public List<TimeSeriesDataPoint> getTimeSeriesRows() { return timeSeriesRows; }

  public int getNumRows() { return timeSeriesRows.size(); }
  public boolean hasNoRows() { return timeSeriesRows.isEmpty(); }
  public long getFirstTimestamp() { return firstTimestamp; }
  public long getLastTimestamp() { return lastTimestamp; }

  public void setFirstTimestamp(long timestamp) {
    this.firstTimestamp = timestamp;
  }
  public void setLastTimestamp(long timestamp) {
    this.lastTimestamp = timestamp;
  }

  /**
   * Checks if cache response is missing any data
   * @param sliceStart original requested start time
   * @param sliceEnd original requested end time
   * @return whether data fetched from cache is missing any data for the original request
   */

  public boolean isMissingSlice(long sliceStart, long sliceEnd) {
    checkAndLogMissingMiddleSlices();
    return isMissingStartSlice(sliceStart) || isMissingEndSlice(sliceEnd);
  }

  /**
   * Checks if cache response is missing data from the start of the time series.
   * @param sliceStart
   * @return true or false
   */
  public boolean isMissingStartSlice(long sliceStart) {
    if (this.hasNoRows()) {
      return true;
    }
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceStart <= getFirstTimestamp() - timeGranularity;
  }

  /**
   * Checks if cache response is missing data from the end of the time series.
   * @param sliceEnd
   * @return true or false
   */
  public boolean isMissingEndSlice(long sliceEnd) {
    if (this.hasNoRows()) {
      return true;
    }

    // note: sliceEnd is exclusive so we don't need equality check.
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();
    return sliceEnd > getLastTimestamp() + timeGranularity;
  }

  /**
   * Checks if there is missing data in the middle of a time-series, and logs them
   * if there are. May be "inaccurate" if the original time-series is also missing
   * data points from the middle of the series.
   */
  private void checkAndLogMissingMiddleSlices() {

    List<String> missingTimestamps = new ArrayList<>();
    long timeGranularity = request.getRequest().getGroupByTimeGranularity().toMillis();

    // remember that we return the cached timeseries in sorted order,
    // but this assumption is not necessarily true if mergeSliceIntoRows() has been called.
    for (int i = 1; i < timeSeriesRows.size(); i++) {
      long previousTimestamp = timeSeriesRows.get(i - 1).getTimestamp();
      long currentTimestamp = timeSeriesRows.get(i).getTimestamp();

      // add all missing timestamps between previous timestamp and current timestamp to
      // the list of missing timestamps.
      while (previousTimestamp + timeGranularity < currentTimestamp) {
        missingTimestamps.add(String.valueOf(previousTimestamp + timeGranularity));
        previousTimestamp += timeGranularity;
      }
    }

    if (missingTimestamps.size() > 0) {
      LOG.info("cached time-series for metricUrn {} was missing data points in the middle for {} timestamps: {}",
          request.getMetricUrn(), missingTimestamps.size(), String.join(",", missingTimestamps));
    }
  }

  /**
   * Used to merge in some new time-series slice into the rows we currently have.
   * The main use case is for when we have a partial cache hit, i.e. only part of the
   * requested data was found in the cache, and need to merge in the new slice that
   * we fetched from the data source to "complete" the time-series. Also updates
   * the first and last timestamps if needed.
   * @param slice response object from fetching from data source
   */

  public void mergeSliceIntoRows(ThirdEyeResponse slice) {

    for (MetricFunction metric : slice.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(slice.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < slice.getNumRowsFor(metric); i++) {
        Map<String, String> row = slice.getRow(metric, i);
        long timestamp = Long.valueOf(row.get(CacheConstants.TIMESTAMP));

        if (timestamp < this.getFirstTimestamp()) {
          this.setFirstTimestamp(timestamp);
        }
        if (timestamp > this.getLastTimestamp()) {
          this.setLastTimestamp(timestamp);
        }

        timeSeriesRows.add(new TimeSeriesDataPoint(metricUrn, timestamp, metric.getMetricId(), row.get(metric.toString())));
      }
    }
  }
}
