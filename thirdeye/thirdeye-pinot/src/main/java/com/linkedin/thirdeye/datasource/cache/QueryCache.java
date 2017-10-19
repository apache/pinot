package com.linkedin.thirdeye.datasource.cache;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;

public class QueryCache {
  private final ExecutorService executorService;
  private final Map<String, ThirdEyeDataSource> dataSourceMap;

  public QueryCache(Map<String, ThirdEyeDataSource> dataSourceMap, ExecutorService executorService) {
    this.executorService = executorService;
    this.dataSourceMap = dataSourceMap;
  }

  public ThirdEyeDataSource getDataSource(String dataSource) {
    return dataSourceMap.get(dataSource);
  }

  public ThirdEyeResponse getQueryResult(ThirdEyeRequest request) throws Exception {
    long tStart = System.nanoTime();
    try {
      String dataSource = request.getDataSource();
      ThirdEyeResponse response = getDataSource(dataSource).execute(request);
      return response;
    } finally {
      ThirdeyeMetricsUtil.datasourceCallCounter.inc();
      ThirdeyeMetricsUtil.datasourceDurationCounter.inc(System.nanoTime() - tStart);
    }
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

  public void clear() throws Exception {
    dataSourceMap.clear();
  }
}
