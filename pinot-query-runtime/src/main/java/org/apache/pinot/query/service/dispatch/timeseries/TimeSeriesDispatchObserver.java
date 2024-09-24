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

import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * Response observer for a time-series query request.
 * TODO: This shouldn't exist and we should re-use DispatchObserver. TBD as part of multi-stage
 *   engine integration.
 */
public class TimeSeriesDispatchObserver implements StreamObserver<Worker.TimeSeriesResponse> {
  private final QueryServerInstance _serverInstance;
  private final Consumer<AsyncQueryTimeSeriesDispatchResponse> _callback;

  private Worker.TimeSeriesResponse _timeSeriesResponse;

  public TimeSeriesDispatchObserver(QueryServerInstance serverInstance,
      Consumer<AsyncQueryTimeSeriesDispatchResponse> callback) {
    _serverInstance = serverInstance;
    _callback = callback;
  }

  @Override
  public void onNext(Worker.TimeSeriesResponse timeSeriesResponse) {
    _timeSeriesResponse = timeSeriesResponse;
  }

  @Override
  public void onError(Throwable throwable) {
    _callback.accept(
        new AsyncQueryTimeSeriesDispatchResponse(
            _serverInstance,
            Worker.TimeSeriesResponse.getDefaultInstance(),
            throwable));
  }

  @Override
  public void onCompleted() {
    _callback.accept(
        new AsyncQueryTimeSeriesDispatchResponse(
            _serverInstance,
            _timeSeriesResponse,
            null));
  }
}
