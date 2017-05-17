package com.linkedin.thirdeye.client.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;

public class QueryCache {
  private final ExecutorService executorService;
  private final Map<String, ThirdEyeClient> clientMap;

  public QueryCache(Map<String, ThirdEyeClient> clientMap, ExecutorService executorService) {
    this.executorService = executorService;
    this.clientMap = clientMap;
  }

  public Map<String, ThirdEyeClient> getClientMap() {
    return clientMap;
  }

  public ThirdEyeClient getClient(String client) {
    return clientMap.get(client);
  }

  public ThirdEyeResponse getQueryResult(ThirdEyeRequest request) throws Exception {
    String client = request.getClient();
    ThirdEyeResponse response = getClient(client).execute(request);
    return response;
  }

  public Future<ThirdEyeResponse> getQueryResultAsync(final ThirdEyeRequest request)
      throws Exception {
    return executorService.submit(new Callable<ThirdEyeResponse>() {
      @Override
      public ThirdEyeResponse call() throws Exception {
        return getQueryResult(request);
      }
    });
  }

  public Map<ThirdEyeRequest, Future<ThirdEyeResponse>> getQueryResultsAsync(
      final List<ThirdEyeRequest> requests) throws Exception {
    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> responseFuturesMap = new LinkedHashMap<>();
    for (final ThirdEyeRequest request : requests) {
      Future<ThirdEyeResponse> responseFuture =
          executorService.submit(new Callable<ThirdEyeResponse>() {
            @Override
            public ThirdEyeResponse call() throws Exception {
              return getQueryResult(request);
            }
          });
      responseFuturesMap.put(request, responseFuture);
    }
    return responseFuturesMap;
  }

  public Map<ThirdEyeRequest, ThirdEyeResponse> getQueryResultsAsyncAndWait(
      final List<ThirdEyeRequest> requests) throws Exception {
    return getQueryResultsAsyncAndWait(requests, 60);
  }

  public Map<ThirdEyeRequest, ThirdEyeResponse> getQueryResultsAsyncAndWait(
      final List<ThirdEyeRequest> requests, int timeoutSeconds) throws Exception {
    Map<ThirdEyeRequest, ThirdEyeResponse> responseMap = new LinkedHashMap<>();
    for (final ThirdEyeRequest request : requests) {
      Future<ThirdEyeResponse> responseFuture =
          executorService.submit(new Callable<ThirdEyeResponse>() {
            @Override
            public ThirdEyeResponse call() throws Exception {
              return getQueryResult(request);
            }
          });
      responseMap.put(request, responseFuture.get(timeoutSeconds, TimeUnit.SECONDS));
    }
    return responseMap;
  }

  public void clear() throws Exception {
    clientMap.clear();
  }

}
