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
package org.apache.pinot.query.context;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.QueryServerInstance;


public class PhysicalPlannerContext {
  private final Supplier<Integer> _nodeIdGenerator = new Supplier<>() {
    private int _id = 0;

    @Override
    public Integer get() {
      return _id++;
    }
  };
  /**
   * This is hacky. We should have a centralized place to get this. This map currently is populated during table scan
   * worker assignment
   */
  private final Map<String, QueryServerInstance> _instanceIdToQueryServerInstance = new HashMap<>();
  @Nullable
  private final RoutingManager _routingManager;
  private final String _hostName;
  private final int _port;
  private final long _requestId;
  private final String _instanceId;

  /**
   * Used by controller when it needs to extract table names from the query.
   * TODO: Controller should only rely on SQL parser to extract table names.
   */
  public PhysicalPlannerContext() {
    _routingManager = null;
    _hostName = "";
    _port = 0;
    _requestId = 0;
    _instanceId = "";
  }

  public PhysicalPlannerContext(RoutingManager routingManager, String hostName, int port, long requestId,
      String instanceId) {
    _routingManager = routingManager;
    _hostName = hostName;
    _port = port;
    _requestId = requestId;
    _instanceId = instanceId;
  }

  public Supplier<Integer> getNodeIdGenerator() {
    return _nodeIdGenerator;
  }

  public Map<String, QueryServerInstance> getInstanceIdToQueryServerInstance() {
    return _instanceIdToQueryServerInstance;
  }

  @Nullable
  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getInstanceId() {
    return _instanceId;
  }
}
