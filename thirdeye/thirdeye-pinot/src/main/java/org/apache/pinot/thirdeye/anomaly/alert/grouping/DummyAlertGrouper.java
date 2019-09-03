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

package org.apache.pinot.thirdeye.anomaly.alert.grouping;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Returns a group that contains all input anomalies. Additionally, this class always returns empty auxiliary email
 * recipients.
 */
public class DummyAlertGrouper extends BaseAlertGrouper {

  @Override
  public Map<DimensionMap, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults) {
    Map<DimensionMap, GroupedAnomalyResultsDTO> groupMap = new HashMap<>();
    GroupedAnomalyResultsDTO groupedAnomalyResults = new GroupedAnomalyResultsDTO();
    groupedAnomalyResults.setAnomalyResults(anomalyResults);
    groupMap.put(new DimensionMap(), groupedAnomalyResults);
    return groupMap;
  }
}
