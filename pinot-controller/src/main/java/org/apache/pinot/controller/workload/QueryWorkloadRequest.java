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
package org.apache.pinot.controller.workload;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.workload.InstanceCost;

/**
 * The request contains a map of workload names to their corresponding instance costs.
 * The request also contains a flag indicating whether the request is for refreshing existing costs.
 * If the request is for refreshing existing costs, the map will contain only the workload names with non-null
 * instance costs. If the request is for deleting costs, the map will contain workload names with null instance costs.
 *
 * For example:
 * 1. To refresh costs for workload "workloadA" and "workloadB", the map will be:
 * <pre>{@code
 *  {
 *    "foo": {"cpuCostNs": 1000000, "memoryCostBytes": 1000000},
 *    "bar": {"cpuCostNs": 500000, "memoryCostBytes": 500000}
 *  }
 * }</pre>
 *
 * 2. To delete costs for workload "workloadC", the map will be:
 * <pre>{@code
 *    "baz": null
 * }</pre>
 */
public class QueryWorkloadRequest {

  private final Map<String, InstanceCost> _workloadToCostMap;
  private final boolean _isRefresh;

  public QueryWorkloadRequest(Map<String, InstanceCost> workloadToCostMap, boolean isRefresh) {
    _workloadToCostMap = workloadToCostMap;
    _isRefresh = isRefresh;
  }


  public QueryWorkloadRequest(String workloadName, @Nullable InstanceCost instanceCost) {
    Map<String, InstanceCost> workloadToCostMap = new HashMap<>();
    workloadToCostMap.put(workloadName, instanceCost);
    _workloadToCostMap = workloadToCostMap;
    _isRefresh = instanceCost != null;
  }

  public Map<String, InstanceCost> getWorkloadToCostMap() {
    return _workloadToCostMap;
  }

  public boolean isRefresh() {
    return _isRefresh;
  }
}
