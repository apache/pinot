package com.linkedin.thirdeye.client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeResponse;

public class QueryCache {
  private final ExecutorService executorService;
  private final ThirdEyeClient client;

  public QueryCache(ThirdEyeClient clientMap, ExecutorService executorService) {
    this.executorService = executorService;
    this.client = clientMap;
  }

  public ThirdEyeClient getClient() {
    return client;
  }
  
  public PinotThirdEyeResponse getQueryResult(ThirdEyeRequest request) throws Exception {
    PinotThirdEyeResponse response = client.execute(request);
    return response;
  }

  public Future<PinotThirdEyeResponse> getQueryResultAsync(final ThirdEyeRequest request)
      throws Exception {
    return executorService.submit(new Callable<PinotThirdEyeResponse>() {
      @Override
      public PinotThirdEyeResponse call() throws Exception {
        return getQueryResult(request);
      }
    });
  }

  public Map<ThirdEyeRequest, Future<PinotThirdEyeResponse>> getQueryResultsAsync(
      final List<ThirdEyeRequest> requests) throws Exception {
    Map<ThirdEyeRequest, Future<PinotThirdEyeResponse>> responseFuturesMap = new LinkedHashMap<>();
    for (final ThirdEyeRequest request : requests) {
      Future<PinotThirdEyeResponse> responseFuture =
          executorService.submit(new Callable<PinotThirdEyeResponse>() {
            @Override
            public PinotThirdEyeResponse call() throws Exception {
              return getQueryResult(request);
            }
          });
      responseFuturesMap.put(request, responseFuture);
    }
    return responseFuturesMap;
  }

  public Map<ThirdEyeRequest, PinotThirdEyeResponse> getQueryResultsAsyncAndWait(
      final List<ThirdEyeRequest> requests) throws Exception {
    return getQueryResultsAsyncAndWait(requests, 60);
  }

  public Map<ThirdEyeRequest, PinotThirdEyeResponse> getQueryResultsAsyncAndWait(
      final List<ThirdEyeRequest> requests, int timeoutSeconds) throws Exception {
    Map<ThirdEyeRequest, PinotThirdEyeResponse> responseMap = new LinkedHashMap<>();
    for (final ThirdEyeRequest request : requests) {
      Future<PinotThirdEyeResponse> responseFuture =
          executorService.submit(new Callable<PinotThirdEyeResponse>() {
            @Override
            public PinotThirdEyeResponse call() throws Exception {
              return getQueryResult(request);
            }
          });
      responseMap.put(request, responseFuture.get(timeoutSeconds, TimeUnit.SECONDS));
    }
    return responseMap;
  }

  public void clear() throws Exception {
    client.clear();
  }

}
