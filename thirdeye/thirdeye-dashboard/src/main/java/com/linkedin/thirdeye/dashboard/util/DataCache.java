package com.linkedin.thirdeye.dashboard.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

public class DataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataCache.class);

  private final ThirdEyeClient client;

  public DataCache(ThirdEyeClient clientMap) {
    this.client = clientMap;
  }

  public CollectionSchema getCollectionSchema(String collection) throws Exception {
    return CollectionSchema.fromStarTreeConfig(client.getStarTreeConfig(collection));

  }

  public List<String> getCollections() throws Exception {
    return client.getCollections();
  }

  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    return client.getSegmentDescriptors(collection);

  }

  public void clear() throws Exception {
    client.clear();
  }

}
