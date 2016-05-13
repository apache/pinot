package com.linkedin.thirdeye.client.cache;

import java.io.InputStream;
import java.net.URLEncoder;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;

public class SchemaCacheLoader extends CacheLoader<String, Schema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCacheLoader.class);

  private static final String UTF_8 = "UTF-8";
  private static final String SCHEMA_ENDPOINT = "schemas/";

  private final CloseableHttpClient controllerClient;
  private final HttpHost controllerHost;

  public SchemaCacheLoader(PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {
    this.controllerClient = HttpClients.createDefault();
    this.controllerHost = new HttpHost(pinotThirdEyeClientConfig.getControllerHost(),
        pinotThirdEyeClientConfig.getControllerPort());
  }

  @Override
  public Schema load(String collection) throws Exception {
    HttpGet req = new HttpGet(SCHEMA_ENDPOINT + URLEncoder.encode(collection, UTF_8));
    LOGGER.info("Retrieving schema: {}", req);
    CloseableHttpResponse res = controllerClient.execute(controllerHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      Schema schema = new ObjectMapper().readValue(content, Schema.class);
      return schema;
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }
}

