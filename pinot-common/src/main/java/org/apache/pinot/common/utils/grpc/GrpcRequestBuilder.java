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
package org.apache.pinot.common.utils.grpc;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;


public class GrpcRequestBuilder {
  private long _requestId;
  private String _brokerId = "unknown";
  private boolean _enableTrace;
  private boolean _enableStreaming;
  private String _payloadType;
  private String _sql;
  private BrokerRequest _brokerRequest;
  private List<String> _segments;

  public GrpcRequestBuilder setRequestId(long requestId) {
    _requestId = requestId;
    return this;
  }

  public GrpcRequestBuilder setBrokerId(String brokerId) {
    _brokerId = brokerId;
    return this;
  }

  public GrpcRequestBuilder setEnableTrace(boolean enableTrace) {
    _enableTrace = enableTrace;
    return this;
  }

  public GrpcRequestBuilder setEnableStreaming(boolean enableStreaming) {
    _enableStreaming = enableStreaming;
    return this;
  }

  public GrpcRequestBuilder setSql(String sql) {
    _payloadType = Request.PayloadType.SQL;
    _sql = sql;
    return this;
  }

  public GrpcRequestBuilder setBrokerRequest(BrokerRequest brokerRequest) {
    _payloadType = Request.PayloadType.BROKER_REQUEST;
    _brokerRequest = brokerRequest;
    return this;
  }

  public GrpcRequestBuilder setSegments(List<String> segments) {
    _segments = segments;
    return this;
  }

  public Server.ServerRequest build() {
    Preconditions.checkState(_payloadType != null && CollectionUtils.isNotEmpty(_segments),
        "Query and segmentsToQuery must be set");

    Map<String, String> metadata = new HashMap<>();
    metadata.put(Request.MetadataKeys.REQUEST_ID, Long.toString(_requestId));
    metadata.put(Request.MetadataKeys.BROKER_ID, _brokerId);
    metadata.put(Request.MetadataKeys.ENABLE_TRACE, Boolean.toString(_enableTrace));
    metadata.put(Request.MetadataKeys.ENABLE_STREAMING, Boolean.toString(_enableStreaming));
    metadata.put(Request.MetadataKeys.PAYLOAD_TYPE, _payloadType);

    if (_payloadType.equals(Request.PayloadType.SQL)) {
      return Server.ServerRequest.newBuilder().putAllMetadata(metadata).setSql(_sql).addAllSegments(_segments).build();
    } else {
      byte[] payLoad;
      try {
        payLoad = new TSerializer(new TCompactProtocol.Factory()).serialize(_brokerRequest);
      } catch (TException e) {
        throw new RuntimeException("Caught exception while serializing broker request: " + _brokerRequest, e);
      }
      return Server.ServerRequest.newBuilder().putAllMetadata(metadata).setPayload(ByteString.copyFrom(payLoad))
          .addAllSegments(_segments).build();
    }
  }
}
