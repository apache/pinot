package com.linkedin.thirdeye.dashboard.util;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataCache.class);

  private static final String ENCODING = "UTF-8";

  private final LoadingCache<String, CollectionSchema> schemas;
  private final LoadingCache<String, List<String>> collections;

  public DataCache(final HttpClient httpClient, final ObjectMapper objectMapper) {
    this.schemas = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.SECONDS)
        .build(new CollectionSchemaCacheLoader(httpClient, objectMapper));
    this.collections = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.SECONDS)
        .build(new CollectionsCacheLoader(httpClient, objectMapper));
  }

  public CollectionSchema getCollectionSchema(String serverUri, String collection) throws Exception {
    return schemas.get(serverUri + "/collections/" + URLEncoder.encode(collection, ENCODING));
  }

  public List<String> getCollections(String serverUri) throws Exception {
    return collections.get(serverUri + "/collections");
  }

  private static class CollectionSchemaCacheLoader extends CacheLoader<String, CollectionSchema> {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    CollectionSchemaCacheLoader(HttpClient httpClient, ObjectMapper objectMapper) {
      this.httpClient = httpClient;
      this.objectMapper = objectMapper;
    }

    @Override
    public CollectionSchema load(String uri) throws Exception {
      CollectionSchema schema = new CollectionSchema();
      HttpGet httpUriRequest = new HttpGet(URI.create(uri));
      HttpResponse response = httpClient.execute(httpUriRequest);
      try {
        JsonNode json = objectMapper.readTree(response.getEntity().getContent());

        // Dimensions
        List<String> dimensions = new ArrayList<>();
        for (JsonNode dimension : json.get("dimensions")) {
          dimensions.add(dimension.get("name").asText());
        }
        schema.setDimensions(dimensions);

        // Metrics
        List<String> metrics = new ArrayList<>();
        for (JsonNode dimension : json.get("metrics")) {
          metrics.add(dimension.get("name").asText());
        }
        schema.setMetrics(metrics);
      } finally {
        EntityUtils.consume(response.getEntity());
      }
      LOGGER.info("Cached schema for {}: {}", uri, schema);
      return schema;
    }
  }

  private static class CollectionsCacheLoader extends CacheLoader<String, List<String>> {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    CollectionsCacheLoader(HttpClient httpClient, ObjectMapper objectMapper) {
      this.httpClient = httpClient;
      this.objectMapper = objectMapper;
    }

    @Override
    public List<String> load(String uri) throws Exception {
      List<String> collections;
      HttpGet httpGet = new HttpGet(URI.create(uri));
      HttpResponse response = httpClient.execute(httpGet);
      try {
        collections = objectMapper.readValue(response.getEntity().getContent(), new TypeReference<List<String>>(){});
      } finally {
        EntityUtils.consume(response.getEntity());
      }
      LOGGER.info("Cached collections for {}: {}", uri, collections);
      return collections;
    }
  }
}
