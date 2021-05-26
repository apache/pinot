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
package org.apache.pinot.controller.api.debug;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import org.apache.commons.io.FileUtils;


@JsonPropertyOrder({"tableName", "numSegments", "numServers", "numBrokers", "segmentDebugInfos", "serverDebugInfos", "brokerDebugInfos"})
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class TableDebugInfo {
  @JsonProperty("tableName")
  private final String _tableName;

  @JsonProperty("tableSize")
  private final TableSizeSummary _tableSizeSummary;

  @JsonProperty("numSegments")
  private final int _numSegments;

  @JsonProperty("numServers")
  private final int _numServers;

  @JsonProperty("numBrokers")
  private final int _numBrokers;

  @JsonProperty("segmentDebugInfos")
  private final List<SegmentDebugInfo> _segmentDebugInfos;

  @JsonProperty("serverDebugInfos")
  private final List<ServerDebugInfo> _serverDebugInfos;

  @JsonProperty("brokerDebugInfos")
  private final List<BrokerDebugInfo> _brokerDebugInfos;

  @JsonCreator
  public TableDebugInfo(String tableName, TableSizeSummary tableSizeSummary, int numBrokers, int numServers,
      int numSegments, List<SegmentDebugInfo> segmentDebugInfos, List<ServerDebugInfo> serverDebugInfos,
      List<BrokerDebugInfo> brokerDebugInfos) {
    _tableName = tableName;
    _tableSizeSummary = tableSizeSummary;

    _numBrokers = numBrokers;
    _numServers = numServers;
    _numSegments = numSegments;

    _segmentDebugInfos = segmentDebugInfos;
    _serverDebugInfos = serverDebugInfos;
    _brokerDebugInfos = brokerDebugInfos;
  }

  public String getTableName() {
    return _tableName;
  }

  public TableSizeSummary getTableSize() {
    return _tableSizeSummary;
  }

  public int getNumSegments() {
    return _numSegments;
  }

  public int getNumServers() {
    return _numServers;
  }

  public int getNumBrokers() {
    return _numBrokers;
  }

  public List<SegmentDebugInfo> getSegmentDebugInfos() {
    return _segmentDebugInfos;
  }

  public List<ServerDebugInfo> getServerDebugInfos() {
    return _serverDebugInfos;
  }

  public List<BrokerDebugInfo> getBrokerDebugInfos() {
    return _brokerDebugInfos;
  }

  public static class SegmentDebugInfo {
    private final String _segmentName;
    private final List<IsEvState> _states;

    public SegmentDebugInfo(String name, List<IsEvState> states) {
      _segmentName = name;
      _states = states;
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public List<IsEvState> getSegmentStateInServer() {
      return _states;
    }
  }

  public static class IsEvState {
    private final String _serverName;
    private final String _idealStateStatus;
    private final String _externalViewStatus;

    public IsEvState(String name, String idealStateStatus, String externalViewStatus) {
      _serverName = name;
      _idealStateStatus = idealStateStatus;
      _externalViewStatus = externalViewStatus;
    }

    public String getServerName() {
      return _serverName;
    }

    public String getIdealStateStatus() {
      return _idealStateStatus;
    }

    public String getExternalViewStatus() {
      return _externalViewStatus;
    }
  }

  public static class ServerDebugInfo {
    private final int _numErrors;
    private final int _numMessages;
    private final String _serverName;

    public ServerDebugInfo(String serverName, int numErrors, int numMessages) {
      _serverName = serverName;
      _numErrors = numErrors;
      _numMessages = numMessages;
    }

    public int getErrors() {
      return _numErrors;
    }

    public int getNumMessages() {
      return _numMessages;
    }

    public String getServerName() {
      return _serverName;
    }
  }

  public static class BrokerDebugInfo {
    private final String _brokerName;
    private final IsEvState _state;

    public BrokerDebugInfo(String brokerName, IsEvState state) {
      _brokerName = brokerName;
      _state = state;
    }

    public String getBrokerName() {
      return _brokerName;
    }

    public IsEvState getState() {
      return _state;
    }
  }

  public static class TableSizeSummary {
    private final String _reportedSize;
    private final String _estimatedSize;

    public TableSizeSummary(long reportedSize, long estimatedSize) {

      _reportedSize = FileUtils.byteCountToDisplaySize(reportedSize);
      _estimatedSize = FileUtils.byteCountToDisplaySize(estimatedSize);
    }

    public String getReportedSize() {
      return _reportedSize;
    }

    public String getEstimatedSize() {
      return _estimatedSize;
    }
  }
}
