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
package org.apache.pinot.tsdb.planner.physical;

import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public class TimeSeriesDispatchablePlan {
  private final TimeSeriesQueryServerInstance _queryServerInstance;
  private final String _language;
  private final String _serializedPlan;
  private final TimeBuckets _timeBuckets;
  private final Map<String, List<String>> _planIdToSegments;

  public TimeSeriesDispatchablePlan(String language, TimeSeriesQueryServerInstance queryServerInstance,
      String serializedPlan, TimeBuckets timeBuckets, Map<String, List<String>> planIdToSegments) {
    _language = language;
    _queryServerInstance = queryServerInstance;
    _serializedPlan = serializedPlan;
    _timeBuckets = timeBuckets;
    _planIdToSegments = planIdToSegments;
  }

  public String getLanguage() {
    return _language;
  }

  public TimeSeriesQueryServerInstance getQueryServerInstance() {
    return _queryServerInstance;
  }

  public String getSerializedPlan() {
    return _serializedPlan;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, List<String>> getPlanIdToSegments() {
    return _planIdToSegments;
  }
}
