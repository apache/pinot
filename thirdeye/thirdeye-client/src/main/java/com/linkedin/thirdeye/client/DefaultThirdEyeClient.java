package com.linkedin.thirdeye.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.factory.DefaultThirdEyeClientFactory;
import com.linkedin.thirdeye.client.util.SqlUtils;

/**
 * Standard client for querying against ThirdEye server. It is strongly recommended to use
 * {@link CachedThirdEyeClient} or instantiate this class via {@link DefaultThirdEyeClientFactory}
 * to improve performance.
 */
public class DefaultThirdEyeClient extends BaseThirdEyeClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultThirdEyeClient.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String COLLECTIONS_ENDPOINT = "/collections/";
  private static final String QUERY_ENDPOINT = "/query/";
  private static final String SEGMENTS_ENDPOINT = "/segments";
  private static final String UTF_8 = "UTF-8";

  private final HttpHost httpHost;
  private final CloseableHttpClient httpClient;

  public DefaultThirdEyeClient(String hostname, int port) {
    LOG.info("Initializing client for {}:{}", hostname, port);
    this.httpHost = new HttpHost(hostname, port);
    // TODO currently no way to configure the CloseableHttpClient
    // use pooled manager if more parallelism required.
    this.httpClient = HttpClients.createDefault();
    LOG.info("Created DefaultThirdEyeClient to {}", httpHost);
  }

  @Override
  public ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception {
    String sql = SqlUtils.getThirdEyeSql(request);
    LOG.debug("Generated SQL {}", sql);
    HttpGet req = new HttpGet(QUERY_ENDPOINT + URLEncoder.encode(sql, UTF_8));
    LOG.info("Executing sql request: {}", req);
    CloseableHttpResponse res = httpClient.execute(httpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }

      // Parse response
      InputStream content = res.getEntity().getContent();
      ThirdEyeRawResponse rawResponse = OBJECT_MAPPER.readValue(content, ThirdEyeRawResponse.class);
      return rawResponse;

    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }

  @Override
  public StarTreeConfig getStarTreeConfig(String collection) throws Exception {
    HttpGet req = new HttpGet(COLLECTIONS_ENDPOINT + URLEncoder.encode(collection, UTF_8));
    LOG.info("Retrieving star tree config: {}", req);
    CloseableHttpResponse res = httpClient.execute(httpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      StarTreeConfig starTreeConfig = OBJECT_MAPPER.readValue(content, StarTreeConfig.class);

      return starTreeConfig;
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }

  @Override
  public List<String> getCollections() throws Exception {
    try {
      HttpGet req = new HttpGet(COLLECTIONS_ENDPOINT);
      LOG.info("Loading collections: {}", req);
      CloseableHttpResponse res = httpClient.execute(httpHost, req);
      try {
        if (res.getStatusLine().getStatusCode() != 200) {
          throw new IllegalStateException(res.getStatusLine().toString());
        }
        InputStream content = res.getEntity().getContent();

        List<String> collections = OBJECT_MAPPER.readValue(content,
            OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
        return collections;
      } finally {
        if (res.getEntity() != null) {
          EntityUtils.consume(res.getEntity());
        }
        res.close();
      }
    } catch (IllegalStateException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    HttpGet req = new HttpGet(
        COLLECTIONS_ENDPOINT + URLEncoder.encode(collection, UTF_8) + SEGMENTS_ENDPOINT);
    LOG.info("Loading segment descriptors: {}", req);
    CloseableHttpResponse res = httpClient.execute(httpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();

      List<SegmentDescriptor> segments = OBJECT_MAPPER.readValue(content, OBJECT_MAPPER
          .getTypeFactory().constructCollectionType(List.class, SegmentDescriptor.class));
      return segments;
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }

  @Override
  public void clear() throws Exception {
  }

  @Override
  public void close() throws Exception {
    httpClient.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 6 && args.length != 7) {
      throw new IllegalArgumentException(
          "usage: host port collection metricFunction startTime endTime [groupBy]");
    }

    String host = args[0];
    int port = Integer.valueOf(args[1]);
    String collection = args[2];
    String metricFunction = args[3];
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[4]);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[5]);

    ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequestBuilder().setCollection(collection)
        .setStartTimeInclusive(startTime).setEndTime(endTime).setMetricFunction(metricFunction);

    if (args.length == 7) {
      requestBuilder.setGroupBy(args[6]);
    }

    // Use builder to leverage cache in case in case multiple queries will be executed.
    ThirdEyeClient client = new DefaultThirdEyeClientFactory().getClient(host, port);
    try {
      Map<DimensionKey, MetricTimeSeries> result = client.execute(requestBuilder.build());
      for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet()) {
        System.out.println(entry.getKey() + " #=> " + entry.getValue());
      }
    } finally {
      client.close();
    }
  }

}
