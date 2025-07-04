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

import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;


public class BrokerGrpcRequestBuilder {
  private long _requestId;
  private String _cid;
  private String _brokerId = "unknown";
  private boolean _enableTrace;
  private boolean _enableStreaming;
  private String _sql;

  public BrokerGrpcRequestBuilder setRequestId(long requestId) {
    _requestId = requestId;
    return this;
  }

  public BrokerGrpcRequestBuilder setCid(String cid) {
    _cid = cid;
    return this;
  }

  public BrokerGrpcRequestBuilder setBrokerId(String brokerId) {
    _brokerId = brokerId;
    return this;
  }

  public BrokerGrpcRequestBuilder setEnableTrace(boolean enableTrace) {
    _enableTrace = enableTrace;
    return this;
  }

  public BrokerGrpcRequestBuilder setEnableStreaming(boolean enableStreaming) {
    _enableStreaming = enableStreaming;
    return this;
  }

  public BrokerGrpcRequestBuilder setSql(String sql) {
    _sql = sql;
    return this;
  }

  public Broker.BrokerRequest build() {
    String requestId = Long.toString(_requestId);
    String cid = _cid == null ? _cid : requestId;
    return Broker.BrokerRequest.newBuilder()
        .putMetadata(Request.MetadataKeys.REQUEST_ID, requestId)
        .putMetadata(Request.MetadataKeys.CORRELATION_ID, cid)
        .putMetadata(Request.MetadataKeys.BROKER_ID, _brokerId)
        .putMetadata(Request.MetadataKeys.ENABLE_TRACE, Boolean.toString(_enableTrace))
        .putMetadata(Request.MetadataKeys.ENABLE_STREAMING, Boolean.toString(_enableStreaming))
        .setSql(_sql).build();
  }
}
