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
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class TimeSeriesExecutionContext {
  private final String _language;
  private final TimeBuckets _initialTimeBuckets;
  private final Map<String, List<String>> _planIdToSegmentsMap;
  private final long _timeoutMs;
  private final Map<String, String> _metadataMap;

  public TimeSeriesExecutionContext(String language, TimeBuckets initialTimeBuckets,
      Map<String, List<String>> planIdToSegmentsMap, long timeoutMs, Map<String, String> metadataMap) {
    _language = language;
    _initialTimeBuckets = initialTimeBuckets;
    _planIdToSegmentsMap = planIdToSegmentsMap;
    _timeoutMs = timeoutMs;
    _metadataMap = metadataMap;
  }

  public String getLanguage() {
    return _language;
  }

  public TimeBuckets getInitialTimeBuckets() {
    return _initialTimeBuckets;
  }

  public Map<String, List<String>> getPlanIdToSegmentsMap() {
    return _planIdToSegmentsMap;
  }

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  public Map<String, String> getMetadataMap() {
    return _metadataMap;
  }
}
