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

import org.apache.pinot.core.transport.ServerInstance;


public class TimeSeriesQueryServerInstance {
  private final String _instanceId;
  private final String _hostname;
  private final int _queryServicePort;
  private final int _queryMailboxPort;

  public TimeSeriesQueryServerInstance(ServerInstance serverInstance) {
    this(serverInstance.getInstanceId(), serverInstance.getHostname(), serverInstance.getQueryServicePort(),
        serverInstance.getQueryMailboxPort());
  }

  public TimeSeriesQueryServerInstance(String instanceId, String hostname, int queryServicePort, int queryMailboxPort) {
    _instanceId = instanceId;
    _hostname = hostname;
    _queryServicePort = queryServicePort;
    _queryMailboxPort = queryMailboxPort;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getQueryServicePort() {
    return _queryServicePort;
  }

  public int getQueryMailboxPort() {
    return _queryMailboxPort;
  }
}
