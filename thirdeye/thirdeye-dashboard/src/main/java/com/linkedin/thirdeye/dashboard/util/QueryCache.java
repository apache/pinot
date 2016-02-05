package com.linkedin.thirdeye.dashboard.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRawResponse;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.dashboard.api.QueryResult;

public class QueryCache {
  private final ExecutorService executorService;
  private final ThirdEyeClient client;

  public QueryCache(ThirdEyeClient clientMap, ExecutorService executorService) {
    this.executorService = executorService;
    this.client = clientMap;
  }

  public QueryResult getQueryResult(ThirdEyeRequest request) throws Exception {
    ThirdEyeRawResponse rawResponse = client.getRawResponse(request);
    return QueryResult.fromThirdEyeResponse(rawResponse);
  }

  public Future<QueryResult> getQueryResultAsync(final ThirdEyeRequest request) throws Exception {
    return executorService.submit(new Callable<QueryResult>() {
      @Override
      public QueryResult call() throws Exception {
        return getQueryResult(request);
      }
    });
  }

  public void clear() throws Exception {
    client.clear();
  }

}
