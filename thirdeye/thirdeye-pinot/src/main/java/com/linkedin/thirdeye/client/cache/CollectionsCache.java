package com.linkedin.thirdeye.client.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jersey.repackaged.com.google.common.collect.Lists;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class CollectionsCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionsCache.class);

  private static final String TABLES_ENDPOINT = "tables/";
  private static final ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private AtomicReference<List<String>> collectionsRef;

  private final CloseableHttpClient controllerClient;
  private final HttpHost controllerHost;

  List<String> blacklist;
  List<String> whitelist;

  public CollectionsCache(PinotThirdEyeClientConfig pinotThirdEyeClientConfig, ThirdEyeConfiguration config) {
    this.collectionsRef = new AtomicReference<>();
    this.controllerClient = HttpClients.createDefault();
    this.controllerHost = new HttpHost(pinotThirdEyeClientConfig.getControllerHost(),
        pinotThirdEyeClientConfig.getControllerPort());

    blacklist = new ArrayList<>();
    whitelist = new ArrayList<>();
    String whitelistCollections = config.getWhitelistCollections();
    String blacklistCollections = config.getBlacklistCollections();
    if (whitelistCollections != null && !whitelistCollections.isEmpty()) {
      whitelist = Lists.newArrayList(whitelistCollections.split(","));
    }
    if (blacklistCollections != null && !blacklistCollections.isEmpty()) {
      blacklist = Lists.newArrayList(blacklistCollections.split(","));
    }
  }


  public List<String> getCollections() {
    return collectionsRef.get();
  }

  public void loadCollections() throws ClientProtocolException, IOException {

    ArrayList<String> collections = new ArrayList<>();

    HttpGet req = new HttpGet(TABLES_ENDPOINT);
    LOGGER.info("Retrieving collections: {}", req);
    CloseableHttpResponse res = controllerClient.execute(controllerHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      JsonNode tables = new ObjectMapper().readTree(content).get("tables");

      ArrayList<String> skippedCollections = new ArrayList<>();
      for (JsonNode table : tables) {
        String collection = table.asText();
        // if whitelist exists, this has to be in whitelist to be processed
        if (!whitelist.isEmpty() && !whitelist.contains(collection)) {
          continue;
        }
        // if present in blacklist, skip
        if (blacklist.contains(collection)) {
          continue;
        }
        try {
          LOGGER.info("Loading collection cache {}", collection);
          CollectionSchema collectionSchema = CACHE_INSTANCE.getCollectionSchemaCache().get(collection);
          if (collectionSchema == null) {
            LOGGER.debug("Skipping collection {} due to null schema", collection);
            skippedCollections.add(collection);
            continue;
          }
        } catch (Exception e) {
          LOGGER.debug("Skipping collection {} due to schema retrieval exception", collection, e);
          skippedCollections.add(collection);
          continue;
        }
        collections.add(collection);
      }

      collectionsRef.set(collections);
      if (!skippedCollections.isEmpty()) {
        LOGGER.info(
            "{} collections were not included because their schemas could not be retrieved: {}",
            skippedCollections.size(), skippedCollections);
      }
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
      res.close();
    }
  }


}

