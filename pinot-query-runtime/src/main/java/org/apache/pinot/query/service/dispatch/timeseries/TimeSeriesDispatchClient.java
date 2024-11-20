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
package org.apache.pinot.query.service.dispatch.timeseries;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * Dispatch client used to dispatch a runnable plan to the server.
 * TODO: This shouldn't exist and we should re-use DispatchClient. TBD as part of multi-stage
 *   engine integration.
 */
public class TimeSeriesDispatchClient {
  // TODO: Note that time-series engine at present uses QueryServer for data transfer from server to broker. This will
  //   be fixed as we integrate with MSE.
  private static final int INBOUND_SIZE_LIMIT = 256 * 1024 * 1024;
  private final ManagedChannel _channel;
  private final PinotQueryWorkerGrpc.PinotQueryWorkerStub _dispatchStub;

  public TimeSeriesDispatchClient(String host, int port) {
    _channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    _dispatchStub = PinotQueryWorkerGrpc.newStub(_channel).withMaxInboundMessageSize(INBOUND_SIZE_LIMIT);
  }

  public ManagedChannel getChannel() {
    return _channel;
  }

  public void submit(Worker.TimeSeriesQueryRequest request, QueryServerInstance virtualServer, Deadline deadline,
      Consumer<AsyncQueryTimeSeriesDispatchResponse> callback) {
    _dispatchStub.withDeadline(deadline).submitTimeSeries(
        request, new TimeSeriesDispatchObserver(virtualServer, callback));
  }
}
