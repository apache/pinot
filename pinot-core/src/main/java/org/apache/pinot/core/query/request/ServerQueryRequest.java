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
package org.apache.pinot.core.query.request;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;


/**
 * The <code>ServerQueryRequest</code> class encapsulates the query related information as well as the query processing
 * context.
 * <p>All segment independent information should be pre-computed and stored in this class to avoid repetitive work on a
 * per segment basis.
 */
public class ServerQueryRequest {
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();

  private final long _requestId;
  private final String _brokerId;
  private final boolean _enableTrace;
  private final boolean _enableStreaming;
  private final List<String> _segmentsToQuery;
  private final QueryContext _queryContext;

  // Timing information for different phases of query execution
  private final TimerContext _timerContext;

  public ServerQueryRequest(InstanceRequest instanceRequest, ServerMetrics serverMetrics, long queryArrivalTimeMs) {
    _requestId = instanceRequest.getRequestId();
    _brokerId = instanceRequest.getBrokerId() != null ? instanceRequest.getBrokerId() : "unknown";
    _enableTrace = instanceRequest.isEnableTrace();
    _enableStreaming = false;
    _segmentsToQuery = instanceRequest.getSearchSegments();
    _queryContext = BrokerRequestToQueryContextConverter.convert(instanceRequest.getQuery());
    _timerContext = new TimerContext(_queryContext.getTableName(), serverMetrics, queryArrivalTimeMs);
  }

  public ServerQueryRequest(Server.ServerRequest serverRequest, ServerMetrics serverMetrics)
      throws Exception {
    long queryArrivalTimeMs = System.currentTimeMillis();

    Map<String, String> metadata = serverRequest.getMetadataMap();
    _requestId = Long.parseLong(metadata.getOrDefault(Request.MetadataKeys.REQUEST_ID, "0"));
    _brokerId = metadata.getOrDefault(Request.MetadataKeys.BROKER_ID, "unknown");
    _enableTrace = Boolean.parseBoolean(metadata.get(Request.MetadataKeys.ENABLE_TRACE));
    _enableStreaming = Boolean.parseBoolean(metadata.get(Request.MetadataKeys.ENABLE_STREAMING));

    _segmentsToQuery = serverRequest.getSegmentsList();

    BrokerRequest brokerRequest;
    String payloadType = metadata.getOrDefault(Request.MetadataKeys.PAYLOAD_TYPE, Request.PayloadType.SQL);
    if (payloadType.equalsIgnoreCase(Request.PayloadType.SQL)) {
      brokerRequest = SQL_COMPILER.compileToBrokerRequest(serverRequest.getSql());
    } else if (payloadType.equalsIgnoreCase(Request.PayloadType.BROKER_REQUEST)) {
      brokerRequest = new BrokerRequest();
      new TDeserializer(new TCompactProtocol.Factory())
          .deserialize(brokerRequest, serverRequest.getPayload().toByteArray());
    } else {
      throw new UnsupportedOperationException("Unsupported payloadType: " + payloadType);
    }
    _queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    _timerContext = new TimerContext(_queryContext.getTableName(), serverMetrics, queryArrivalTimeMs);
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public boolean isEnableTrace() {
    return _enableTrace;
  }

  public boolean isEnableStreaming() {
    return _enableStreaming;
  }

  public String getTableNameWithType() {
    return _queryContext.getTableName();
  }

  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public QueryContext getQueryContext() {
    return _queryContext;
  }

  public TimerContext getTimerContext() {
    return _timerContext;
  }
}
