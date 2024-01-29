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
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.spi.config.table.TableStatus;


/**
 * This class represents debug information associated with a table. For example:
 * <ul>
 *   <li>Table name, size, number of brokers, servers, segments, etc.</li>
 *   <li>Debug information from server/brokers of the table.</li>
 *   <li>Debug information related to segments of the table.</li>
 * </ul>
 */
@JsonPropertyOrder({
    "tableName", "numSegments", "numServers", "numBrokers", "segmentDebugInfos", "serverDebugInfos", "brokerDebugInfos"
})
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class TableDebugInfo {
  @JsonProperty("tableName")
  private final String _tableName;

  @JsonProperty("ingestionStatus")
  private final TableStatus.IngestionStatus _ingestionStatus;

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
  public TableDebugInfo(String tableName, TableStatus.IngestionStatus ingestionStatus,
      TableSizeSummary tableSizeSummary, int numBrokers, int numServers, int numSegments,
      List<SegmentDebugInfo> segmentDebugInfos, List<ServerDebugInfo> serverDebugInfos,
      List<BrokerDebugInfo> brokerDebugInfos) {
    _tableName = tableName;
    _ingestionStatus = ingestionStatus;
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

  public TableStatus.IngestionStatus getIngestionStatus() {
    return _ingestionStatus;
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

  @JsonPropertyOrder({"segmentName", "serverState", "validationError"})
  public static class SegmentDebugInfo {
    private final String _segmentName;

    private final Map<String, SegmentState> _serverStateMap;

    private final SegmentErrorInfo _validationError;

    @JsonCreator
    public SegmentDebugInfo(@JsonProperty("segmentName") String segmentName,
        @JsonProperty("serverState") Map<String, SegmentState> segmentServerState,
        @JsonProperty("validationError") SegmentErrorInfo validationError) {
      _segmentName = segmentName;
      _serverStateMap = segmentServerState;
      _validationError = validationError;
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public Map<String, SegmentState> getServerState() {
      return _serverStateMap;
    }

    public SegmentErrorInfo getValidationError() {
      return _validationError;
    }
  }

  /**
   * This class represents the state of segment on the server:
   *
   * <ul>
   *   <li>Ideal State vs External view.</li>
   *   <li>Segment related errors and consumer information.</li>
   *   <li>Segment size.</li>
   * </ul>
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"idealState", "externalView", "segmentSize", "consumerInfo", "errorInfo"})
  public static class SegmentState {
    private final String _idealState;
    private final String _externalView;
    private final String _segmentSize;
    private final SegmentConsumerInfo _consumerInfo;
    private final SegmentErrorInfo _errorInfo;

    @JsonCreator
    public SegmentState(@JsonProperty("idealState") String idealState,
        @JsonProperty("externalView") String externalView, @JsonProperty("segmentSize") String segmentSize,
        @JsonProperty("consumerInfo") SegmentConsumerInfo consumerInfo,
        @JsonProperty("errorInfo") SegmentErrorInfo errorInfo) {
      _idealState = idealState;
      _externalView = externalView;
      _segmentSize = segmentSize;
      _consumerInfo = consumerInfo;
      _errorInfo = errorInfo;
    }

    public String getIdealState() {
      return _idealState;
    }

    public String getExternalView() {
      return _externalView;
    }

    public SegmentConsumerInfo getConsumerInfo() {
      return _consumerInfo;
    }

    public SegmentErrorInfo getErrorInfo() {
      return _errorInfo;
    }

    public String getSegmentSize() {
      return _segmentSize;
    }
  }

  /**
   * Debug information related to Server.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"serverName", "numErrors", "numMessages"})
  public static class ServerDebugInfo {
    private final String _serverName;
    private final int _numErrors;
    private final int _numMessages;

    @JsonCreator
    public ServerDebugInfo(@JsonProperty("serverName") String serverName, @JsonProperty("numErrors") int numErrors,
        @JsonProperty("numMessages") int numMessages) {
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

  /**
   * Debug information related to broker.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"brokerName", "state"})
  public static class BrokerDebugInfo {
    private final String _brokerName;
    private final String _idealState;
    private final String _externalView;

    @JsonCreator
    public BrokerDebugInfo(@JsonProperty("brokerName") String brokerName, @JsonProperty("idealState") String idealState,
        @JsonProperty("externalView") String externalView) {
      _brokerName = brokerName;
      _idealState = idealState;
      _externalView = externalView;
    }

    public String getBrokerName() {
      return _brokerName;
    }

    public String getIdealState() {
      return _idealState;
    }

    public String getExternalView() {
      return _externalView;
    }
  }

  /**
   * Summary of table size - reported and estimated size.
   */
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
