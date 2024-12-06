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
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesDispatchablePlan {
  private final List<TimeSeriesQueryServerInstance> _queryServerInstances;
  private final String _language;
  private final BaseTimeSeriesPlanNode _brokerFragment;
  private final List<Pair<String, String>> _serializedPlanFragments;
  private final TimeBuckets _timeBuckets;
  private final Map<String, Map<String, List<String>>> _planIdToSegmentsByServer;
  private final Map<String, Integer> _numInputServersForExchangePlanNode;

  public TimeSeriesDispatchablePlan(String language, List<TimeSeriesQueryServerInstance> queryServerInstances,
      BaseTimeSeriesPlanNode brokerFragment, List<Pair<String, String>> serializedPlanFragmentsByPlanId,
      TimeBuckets initialTimeBuckets, Map<String, Map<String, List<String>>> planIdToSegmentsByServer,
      Map<String, Integer> numInputServersForExchangePlanNode) {
    _language = language;
    _queryServerInstances = queryServerInstances;
    _brokerFragment = brokerFragment;
    _serializedPlanFragments = serializedPlanFragmentsByPlanId;
    _timeBuckets = initialTimeBuckets;
    _planIdToSegmentsByServer = planIdToSegmentsByServer;
    _numInputServersForExchangePlanNode = numInputServersForExchangePlanNode;
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

  public List<String> getSerializedPlanFragments(String instanceId) {
    Set<String> planIdForInstance = _planIdToSegmentsByServer.get(instanceId).keySet();
    List<String> result = new ArrayList<>();
    for (var planIdAndPlanFragment : _serializedPlanFragments) {
      if (planIdForInstance.contains(planIdAndPlanFragment.getLeft())) {
        result.add(planIdAndPlanFragment.getRight());
      }
    }
    return result;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<String, Map<String, List<String>>> getPlanIdToSegmentsByServer() {
    return _planIdToSegmentsByServer;
  }

  public Map<String, Integer> getNumInputServersForExchangePlanNode() {
    return _numInputServersForExchangePlanNode;
  }
}
