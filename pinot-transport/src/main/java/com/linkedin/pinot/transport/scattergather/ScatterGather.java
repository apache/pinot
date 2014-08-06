package com.linkedin.pinot.transport.scattergather;

import io.netty.buffer.ByteBuf;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.CompositeFuture;


/**
 * Asynchronous Scatter-Gather Request API
 *
 */
public interface ScatterGather {
  /**
   * Async Scatter-Gather request.
   * 
   * If one of the request is to the server which is running this code, then the
   * {@link LocalRequestHandler} instance passed as part of scatter request
   * will be called in a separate thread.
   * 
   *  Here are the steps:
   *
   * 1. Selection-strategy is applied to pick one service for each partition-group (PARTITION_GROUP granularity) or
   *    partition (PARTITION granularity).
   * 2. If more than one partitions selects a service, the requests are merged so that there is one request per
   *    service to be queried.
   * 2. Async dispatch of request to each service and track each futures
   * 3. Return an aggregate future that encapsulate individual futures.
   * @param scatterRequest Scatter request
   * @return future containing response from all services queried. The response is in ByteBuf ( not the byte[] ).
   * Hence, the client is responsible for calling release() on the response.
   */
  public CompositeFuture<ServerInstance, ByteBuf> scatterGather(ScatterGatherRequest scatterRequest)
      throws InterruptedException;
}
