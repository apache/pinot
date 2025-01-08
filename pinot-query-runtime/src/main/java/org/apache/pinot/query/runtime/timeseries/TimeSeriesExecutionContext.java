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
package org.apache.pinot.query.runtime.timeseries;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;


public class TimeSeriesExecutionContext {
  private final String _language;
  private final TimeBuckets _initialTimeBuckets;
  private final Map<String, List<String>> _planIdToSegmentsMap;
  private final Map<String, BlockingQueue<Object>> _exchangeReceiverByPlanId;
  private final long _deadlineMs;
  private final Map<String, String> _metadataMap;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesExecutionContext(String language, TimeBuckets initialTimeBuckets, long deadlineMs,
      Map<String, String> metadataMap, Map<String, List<String>> planIdToSegmentsMap,
      Map<String, BlockingQueue<Object>> exchangeReceiverByPlanId) {
    _language = language;
    _initialTimeBuckets = initialTimeBuckets;
    _deadlineMs = deadlineMs;
    _metadataMap = metadataMap;
    _planIdToSegmentsMap = planIdToSegmentsMap;
    _exchangeReceiverByPlanId = exchangeReceiverByPlanId;
    _seriesBuilderFactory = TimeSeriesBuilderFactoryProvider.getSeriesBuilderFactory(language);
  }

  public String getLanguage() {
    return _language;
  }

  public TimeBuckets getInitialTimeBuckets() {
    return _initialTimeBuckets;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public long getRemainingTimeMs() {
    return _deadlineMs - System.currentTimeMillis();
  }

  public Map<String, String> getMetadataMap() {
    return _metadataMap;
  }

  public Map<String, List<String>> getPlanIdToSegmentsMap() {
    return _planIdToSegmentsMap;
  }

  public Map<String, BlockingQueue<Object>> getExchangeReceiverByPlanId() {
    return _exchangeReceiverByPlanId;
  }

  public TimeSeriesBuilderFactory getSeriesBuilderFactory() {
    return _seriesBuilderFactory;
  }
}
