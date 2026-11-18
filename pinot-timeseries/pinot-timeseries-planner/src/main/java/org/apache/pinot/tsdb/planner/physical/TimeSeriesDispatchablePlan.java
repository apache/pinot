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

import java.util.ArrayList;
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
  private final boolean _wholePlanOnOneServer;
  private final TimeBuckets _timeBuckets;
  private final Map<String, Map<String, List<String>>> _leafIdToSegmentsByInstanceId;
  private final Map<String, Map<String, List<String>>> _leafIdToOptionalSegmentsByInstanceId;
  private final Map<String, Integer> _numInputServersForExchangePlanNode;
  private final List<String> _serializedServerFragments;
  private final List<String> _tableNames;

  /// @param wholePlanOnOneServer whether the query's one server runs the whole plan, which needs it to hold segments
  ///     for every leaf; the server fragments are then the whole plan rather than one fragment per leaf
  /// @param leafIdToSegmentsByInstanceId per server, the non-empty required segments of each leaf it reads
  /// @param leafIdToOptionalSegmentsByInstanceId per server, the optional segments of leaves with required segments
  public TimeSeriesDispatchablePlan(String language, List<TimeSeriesQueryServerInstance> queryServerInstances,
      BaseTimeSeriesPlanNode brokerFragment, List<BaseTimeSeriesPlanNode> serverFragments,
      boolean wholePlanOnOneServer, TimeBuckets initialTimeBuckets,
      Map<String, Map<String, List<String>>> leafIdToSegmentsByInstanceId,
      Map<String, Map<String, List<String>>> leafIdToOptionalSegmentsByInstanceId,
      Map<String, Integer> numInputServersForExchangePlanNode, List<String> tableNames) {
    _language = language;
    _queryServerInstances = queryServerInstances;
    _brokerFragment = brokerFragment;
    _serverFragments = serverFragments;
    _wholePlanOnOneServer = wholePlanOnOneServer;
    _timeBuckets = initialTimeBuckets;
    _leafIdToSegmentsByInstanceId = leafIdToSegmentsByInstanceId;
    _leafIdToOptionalSegmentsByInstanceId = leafIdToOptionalSegmentsByInstanceId;
    _numInputServersForExchangePlanNode = numInputServersForExchangePlanNode;
    _serializedServerFragments = serverFragments.stream().map(TimeSeriesPlanSerde::serialize).collect(
        Collectors.toList());
    _tableNames = tableNames;
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

  /// The serialized fragments a server runs: the whole plan when the query's one server holds segments for every leaf,
  /// else the fragments of the leaves it holds segments for. A server sends one block per fragment it runs, and each
  /// exchange waits for one block from each server holding its leaf, so a server that ran a leaf it holds no segments
  /// for would send an extra block that the exchange could take in place of a server's real data.
  public List<String> getSerializedServerFragments(String instanceId) {
    if (_wholePlanOnOneServer) {
      return _serializedServerFragments;
    }
    Map<String, List<String>> segmentsByLeafId = _leafIdToSegmentsByInstanceId.getOrDefault(instanceId, Map.of());
    List<String> fragments = new ArrayList<>();
    for (int i = 0; i < _serverFragments.size(); i++) {
      if (segmentsByLeafId.containsKey(_serverFragments.get(i).getId())) {
        fragments.add(_serializedServerFragments.get(i));
      }
    }
    return fragments;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, Map<String, List<String>>> getLeafIdToSegmentsByInstanceId() {
    return _leafIdToSegmentsByInstanceId;
  }

  public Map<String, Map<String, List<String>>> getLeafIdToOptionalSegmentsByInstanceId() {
    return _leafIdToOptionalSegmentsByInstanceId;
  }

  public Map<String, Integer> getNumInputServersForExchangePlanNode() {
    return _numInputServersForExchangePlanNode;
  }

  public List<String> getTableNames() {
    return _tableNames;
  }
}
