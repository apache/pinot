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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.timeseries.serde.TimeSeriesBlockSerde;
import org.apache.pinot.tsdb.planner.TimeSeriesPlanConstants.WorkerResponseMetadataKeys;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Response observer for a time-series query request.
 * TODO: This shouldn't exist and we should re-use DispatchObserver. TBD as part of multi-stage
 *   engine integration.
 */
public class TimeSeriesDispatchObserver implements StreamObserver<Worker.TimeSeriesResponse> {
  /**
   * Each server should send data for each leaf node once. This capacity controls the size of the queue we use to
   * buffer the data sent by the sender. This is set large enough that we should never hit this for any practical
   * use-case, while guarding us against bugs.
   */
  public static final int MAX_QUEUE_CAPACITY = 4096;
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesDispatchObserver.class);
  private final Map<String, BlockingQueue<Object>> _exchangeReceiversByPlanId;

  public TimeSeriesDispatchObserver(Map<String, BlockingQueue<Object>> exchangeReceiversByPlanId) {
    _exchangeReceiversByPlanId = exchangeReceiversByPlanId;
  }

  @Override
  public void onNext(Worker.TimeSeriesResponse timeSeriesResponse) {
    if (timeSeriesResponse.containsMetadata(WorkerResponseMetadataKeys.ERROR_TYPE)) {
      String errorType = timeSeriesResponse.getMetadataOrDefault(WorkerResponseMetadataKeys.ERROR_TYPE, "");
      String errorMessage = timeSeriesResponse.getMetadataOrDefault(WorkerResponseMetadataKeys.ERROR_MESSAGE, "");
      onError(new Throwable(String.format("Error in server (type: %s): %s", errorType, errorMessage)));
      return;
    }
    String planId = timeSeriesResponse.getMetadataMap().get(WorkerResponseMetadataKeys.PLAN_ID);
    TimeSeriesBlock block = null;
    Throwable error = null;
    try {
      block = TimeSeriesBlockSerde.deserializeTimeSeriesBlock(timeSeriesResponse.getPayload().asReadOnlyByteBuffer());
    } catch (Throwable t) {
      error = t;
    }
    BlockingQueue<Object> receiverForPlanId = _exchangeReceiversByPlanId.get(planId);
    if (receiverForPlanId == null) {
      String message = String.format("Receiver is not initialized for planId: %s. Receivers exist only for planIds: %s",
          planId, _exchangeReceiversByPlanId.keySet());
      LOGGER.warn(message);
      onError(new IllegalStateException(message));
    } else {
      if (!receiverForPlanId.offer(error != null ? error : block)) {
        onError(new RuntimeException(String.format("Offer to receiver queue (capacity=%s) for planId: %s failed",
            receiverForPlanId.remainingCapacity(), planId)));
      }
    }
  }

  @Override
  public void onError(Throwable throwable) {
    for (BlockingQueue q : _exchangeReceiversByPlanId.values()) {
      q.offer(throwable);
    }
  }

  @Override
  public void onCompleted() {
  }
}
