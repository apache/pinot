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
package org.apache.pinot.core.transport.grpc;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.streaming.StreamingResponseUtils;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;


public class GrpcResultsBlockStreamer implements ResultsBlockStreamer {
  private final StreamObserver<Server.ServerResponse> _streamObserver;
  private final ServerMetrics _serverMetrics;

  public GrpcResultsBlockStreamer(StreamObserver<Server.ServerResponse> streamObserver, ServerMetrics serverMetrics) {
    _streamObserver = streamObserver;
    _serverMetrics = serverMetrics;
  }

  @Override
  public void send(BaseResultsBlock block)
      throws IOException {
    Server.ServerResponse response = StreamingResponseUtils.getDataResponse(block.getDataTable());
    _streamObserver.onNext(response);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.GRPC_BYTES_SENT, response.getSerializedSize());
  }
}
