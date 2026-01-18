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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


/**
 * Response object for the GET /minions/status endpoint.
 * Provides status information for all minion instances including their task counts and drain state.
 */
public class MinionStatusResponse {
  private int _currentMinionCount;
  private List<MinionStatus> _minionStatus;

  public MinionStatusResponse() {
  }

  public MinionStatusResponse(int currentMinionCount, List<MinionStatus> minionStatus) {
    _currentMinionCount = currentMinionCount;
    _minionStatus = minionStatus;
  }

  @JsonProperty("currentMinionCount")
  public int getCurrentMinionCount() {
    return _currentMinionCount;
  }

  public void setCurrentMinionCount(int currentMinionCount) {
    _currentMinionCount = currentMinionCount;
  }

  @JsonProperty("minionStatus")
  public List<MinionStatus> getMinionStatus() {
    return _minionStatus;
  }

  public void setMinionStatus(List<MinionStatus> minionStatus) {
    _minionStatus = minionStatus;
  }

  /**
   * Status information for a single minion instance.
   */
  public static class MinionStatus {
    private String _instanceId;
    private String _host;
    private int _port;
    private int _runningTaskCount;
    private String _status;

    public MinionStatus() {
    }

    public MinionStatus(String instanceId, String host, int port, int runningTaskCount, String status) {
      _instanceId = instanceId;
      _host = host;
      _port = port;
      _runningTaskCount = runningTaskCount;
      _status = status;
    }

    @JsonProperty("instanceId")
    public String getInstanceId() {
      return _instanceId;
    }

    public void setInstanceId(String instanceId) {
      _instanceId = instanceId;
    }

    @JsonProperty("host")
    public String getHost() {
      return _host;
    }

    public void setHost(String host) {
      _host = host;
    }

    @JsonProperty("port")
    public int getPort() {
      return _port;
    }

    public void setPort(int port) {
      _port = port;
    }

    @JsonProperty("runningTaskCount")
    public int getRunningTaskCount() {
      return _runningTaskCount;
    }

    public void setRunningTaskCount(int runningTaskCount) {
      _runningTaskCount = runningTaskCount;
    }

    @JsonProperty("status")
    public String getStatus() {
      return _status;
    }

    public void setStatus(String status) {
      _status = status;
    }
  }
}
