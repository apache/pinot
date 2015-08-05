package com.linkedin.thirdeye.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DefaultThirdEyeClient implements ThirdEyeClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultThirdEyeClient.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final HttpHost httpHost;
  private final CloseableHttpClient httpClient;
  private final LoadingCache<QuerySpec, Map<DimensionKey, MetricTimeSeries>> resultCache;
  private final LoadingCache<String, Map<String, MetricType>> schemaCache;

  public DefaultThirdEyeClient(String hostname, int port) {
    this(hostname, port, new DefaultThirdEyeClientConfig());
  }

  @SuppressWarnings("unchecked")
  public DefaultThirdEyeClient(String hostname, int port, DefaultThirdEyeClientConfig config) {
    this.httpHost = new HttpHost(hostname, port);
    this.httpClient = HttpClients.createDefault();

    CacheBuilder builder = CacheBuilder.newBuilder();
    if (config.isExpireAfterAccess()) {
      builder.expireAfterAccess(config.getExpirationTime(), config.getExpirationUnit());
    } else {
      builder.expireAfterWrite(config.getExpirationTime(), config.getExpirationUnit());
    }
    this.resultCache = builder.build(new ResultCacheLoader());

    this.schemaCache = CacheBuilder.newBuilder()
        .expireAfterWrite(Long.MAX_VALUE, TimeUnit.MILLISECONDS) // never
        .build(new SchemaCacheLoader());

    LOG.info("Created DefaultThirdEyeClient to {}", httpHost);
  }

  @Override
  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    QuerySpec querySpec = new QuerySpec(request.getCollection(), request.toSql());
    LOG.debug("Generated SQL {}", request.toSql());
    return resultCache.get(querySpec);
  }

  @Override
  public void close() throws Exception {
    httpClient.close();
  }

  /**
   * Executes SQL statements against the /query resource.
   */
  private class ResultCacheLoader extends CacheLoader<QuerySpec, Map<DimensionKey, MetricTimeSeries>> {
    @Override
    public Map<DimensionKey, MetricTimeSeries> load(QuerySpec querySpec) throws Exception {
      HttpGet req = new HttpGet("/query/" + URLEncoder.encode(querySpec.getSql(), "UTF-8"));
      CloseableHttpResponse res = httpClient.execute(httpHost, req);
      try {
        if (res.getStatusLine().getStatusCode() != 200) {
          throw new IllegalStateException(res.getStatusLine().toString());
        }

        // Parse response
        InputStream content = res.getEntity().getContent();
        ThirdEyeRawResponse rawResponse = OBJECT_MAPPER.readValue(content, ThirdEyeRawResponse.class);

        // Figure out the metric types of the projection
        Map<String, MetricType> metricTypes = schemaCache.get(querySpec.getCollection());
        List<MetricType> projectionTypes = new ArrayList<>();
        for (String metricName : rawResponse.getMetrics()) {
          MetricType metricType = metricTypes.get(metricName);
          if (metricType == null) { // could be derived
            metricType = MetricType.DOUBLE;
          }
          projectionTypes.add(metricTypes.get(metricName));
        }

        return rawResponse.convert(projectionTypes);
      } finally {
        if (res.getEntity() != null) {
          EntityUtils.consume(res.getEntity());
        }
        res.close();
      }
    }
  }

  private class SchemaCacheLoader extends CacheLoader<String, Map<String, MetricType>> {
    @Override
    public Map<String, MetricType> load(String collection) throws Exception {
      HttpGet req = new HttpGet("/collections/" + URLEncoder.encode(collection, "UTF-8"));
      CloseableHttpResponse res = httpClient.execute(httpHost, req);
      try {
        if (res.getStatusLine().getStatusCode() != 200) {
          throw new IllegalStateException(res.getStatusLine().toString());
        }
        InputStream content = res.getEntity().getContent();
        JsonNode json = OBJECT_MAPPER.readTree(content);

        Map<String, MetricType> metricTypes = new HashMap<>();
        for (JsonNode metricSpec : json.get("metrics")) {
          String metricName = metricSpec.get("name").asText();
          MetricType metricType = MetricType.valueOf(metricSpec.get("type").asText());
          metricTypes.put(metricName, metricType);
        }

        LOG.info("Cached metric types for {}: {}", collection, metricTypes);
        return metricTypes;
      } finally {
        if (res.getEntity() != null) {
          EntityUtils.consume(res.getEntity());
        }
        res.close();
      }
    }
  }


  private static class QuerySpec {
    private String collection;
    private String sql;

    QuerySpec(String collection, String sql) {
      this.collection = collection;
      this.sql = sql;
    }

    public String getCollection() {
      return collection;
    }

    public String getSql() {
      return sql;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof QuerySpec)) {
        return false;
      }
      QuerySpec s = (QuerySpec) o;
      return Objects.equals(sql, s.getSql()) && Objects.equals(collection, s.getCollection());
    }

    @Override
    public int hashCode() {
      return Objects.hash(sql, collection);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 6 && args.length != 7) {
      throw new IllegalArgumentException("usage: host port collection metricFunction startTime endTime [groupBy]");
    }

    String host = args[0];
    int port = Integer.valueOf(args[1]);
    String collection = args[2];
    String metricFunction = args[3];
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[4]);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[5]);

    ThirdEyeRequest request = new ThirdEyeRequest()
        .setCollection(collection)
        .setStartTime(startTime)
        .setEndTime(endTime)
        .setMetricFunction(metricFunction);

    if (args.length == 7) {
      request.setGroupBy(args[6]);
    }

    ThirdEyeClient client = new DefaultThirdEyeClient(host, port);
    try {
      Map<DimensionKey, MetricTimeSeries> result = client.execute(request);
      for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet()) {
        System.out.println(entry.getKey() + " #=> " + entry.getValue());
      }
    } finally {
      client.close();
    }
  }
}
