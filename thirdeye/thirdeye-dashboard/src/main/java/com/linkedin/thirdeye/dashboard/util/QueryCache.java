package com.linkedin.thirdeye.dashboard.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class QueryCache {
  private final ExecutorService executorService;
  private final LoadingCache<QuerySpec, QueryResult> cache;

  public QueryCache(HttpClient httpClient, ObjectMapper objectMapper, ExecutorService executorService) {
    this.executorService = executorService;
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new QueryCacheLoader(httpClient, objectMapper));
  }

  public QueryResult getQueryResult(String serverUri, String sql) throws Exception {
    return cache.get(new QuerySpec(serverUri, sql));
  }

  public Future<QueryResult> getQueryResultAsync(final String serverUri, final String sql) throws Exception {
    return executorService.submit(new Callable<QueryResult>() {
      @Override
      public QueryResult call() throws Exception {
        return getQueryResult(serverUri, sql);
      }
    });
  }

  private static class QueryCacheLoader extends CacheLoader<QuerySpec, QueryResult> {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    QueryCacheLoader(HttpClient httpClient, ObjectMapper objectMapper) {
      this.httpClient = httpClient;
      this.objectMapper = objectMapper;
    }

    @Override
    public QueryResult load(QuerySpec querySpec) throws Exception {
      URI uri = URI.create(querySpec.getServerUri() + "/query/" + URLEncoder.encode(querySpec.getSql(), "UTF-8"));
      HttpGet httpGet = new HttpGet(uri);
      HttpResponse httpResponse = httpClient.execute(httpGet);

      if (httpResponse.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(httpResponse.getStatusLine().toString());
      }

      try {
        return objectMapper.readValue(httpResponse.getEntity().getContent(), QueryResult.class);
      } finally {
        EntityUtils.consume(httpResponse.getEntity());
      }
    }
  }

  private static class QuerySpec {
    private final String serverUri;
    private final String sql;

    QuerySpec(String serverUri, String sql) {
      this.serverUri = serverUri;
      this.sql = sql;
    }

    public String getServerUri() {
      return serverUri;
    }

    public String getSql() {
      return sql;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(serverUri, sql);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(QuerySpec.class)
          .add("serverUri", serverUri)
          .add("sql", sql)
          .toString();
    }
  }
}
