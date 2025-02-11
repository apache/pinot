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
import java.util.stream.Collectors;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;


public class TimeSeriesDispatchablePlan {
  private final List<TimeSeriesQueryServerInstance> _queryServerInstances;
  private final String _language;
  private final BaseTimeSeriesPlanNode _brokerFragment;
  private final List<BaseTimeSeriesPlanNode> _serverFragments;
  private final TimeBuckets _timeBuckets;
  private final Map<String, Map<String, List<String>>> _leafIdToSegmentsByInstanceId;
  private final Map<String, Integer> _numInputServersForExchangePlanNode;
  private final List<String> _serializedServerFragments;

  public TimeSeriesDispatchablePlan(String language, List<TimeSeriesQueryServerInstance> queryServerInstances,
      BaseTimeSeriesPlanNode brokerFragment, List<BaseTimeSeriesPlanNode> serverFragments,
      TimeBuckets initialTimeBuckets, Map<String, Map<String, List<String>>> leafIdToSegmentsByInstanceId,
      Map<String, Integer> numInputServersForExchangePlanNode) {
    _language = language;
    _queryServerInstances = queryServerInstances;
    _brokerFragment = brokerFragment;
    _serverFragments = serverFragments;
    _timeBuckets = initialTimeBuckets;
    _leafIdToSegmentsByInstanceId = leafIdToSegmentsByInstanceId;
    _numInputServersForExchangePlanNode = numInputServersForExchangePlanNode;
    _serializedServerFragments = serverFragments.stream().map(TimeSeriesPlanSerde::serialize).collect(
        Collectors.toList());
  }

  public String getLanguage() {
    return _language;
  }

  public List<TimeSeriesQueryServerInstance> getQueryServerInstances() {
    return _queryServerInstances;
  }

  public BaseTimeSeriesPlanNode getBrokerFragment() {
    return _brokerFragment;
  }

  public List<BaseTimeSeriesPlanNode> getServerFragments() {
    return _serverFragments;
  }

  public List<String> getSerializedServerFragments() {
    return _serializedServerFragments;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, Map<String, List<String>>> getLeafIdToSegmentsByInstanceId() {
    return _leafIdToSegmentsByInstanceId;
  }

  public Map<String, Integer> getNumInputServersForExchangePlanNode() {
    return _numInputServersForExchangePlanNode;
  }
}
