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
package org.apache.pinot.core.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.transport.ServerInstance;


public class RoutingTable {
  private final Map<ServerInstance, Map<Integer, List<String>>> _serverInstanceToPartitionedSegmentsMap;
  private final Map<ServerInstance, Integer> _serverInstancePartitionMap;
  private final List<String> _unavailableSegments;
  private final int _numPrunedSegments;

  public RoutingTable(Map<ServerInstance, List<String>> serverInstanceToSegmentsMap,
      List<String> unavailableSegments, int numPrunedSegments) {
    _serverInstanceToPartitionedSegmentsMap = new HashMap<>();
    serverInstanceToSegmentsMap.forEach((key, value) -> {
      Map<Integer, List<String>> partitionedSegments = new HashMap<>();
      partitionedSegments.put(0, value);
      _serverInstanceToPartitionedSegmentsMap.put(key, partitionedSegments);
    });
    _unavailableSegments = unavailableSegments;
    _numPrunedSegments = numPrunedSegments;
    _serverInstancePartitionMap = null;
  }

  public RoutingTable(Map<ServerInstance, Map<Integer, List<String>>> serverInstanceToPartitionedSegmentsMap,
      List<String> unavailableSegments, int numPrunedSegments,
      @Nullable Map<ServerInstance, Integer> serverInstancePartitionMap) {
    _serverInstanceToPartitionedSegmentsMap = serverInstanceToPartitionedSegmentsMap;
    _unavailableSegments = unavailableSegments;
    _numPrunedSegments = numPrunedSegments;
    _serverInstancePartitionMap = serverInstancePartitionMap;
  }

  public Map<ServerInstance, Map<Integer, List<String>>> getServerInstanceToSegmentsMap() {
    return _serverInstanceToPartitionedSegmentsMap;
  }

  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  public int getNumPrunedSegments() {
    return _numPrunedSegments;
  }

  public Map<ServerInstance, Integer> getServerInstancePartitionMap() {
    return _serverInstancePartitionMap;
  }
}
