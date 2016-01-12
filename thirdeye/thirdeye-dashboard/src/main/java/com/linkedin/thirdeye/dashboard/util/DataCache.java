package com.linkedin.thirdeye.dashboard.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

public class DataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataCache.class);

  private static final String ENCODING = "UTF-8";

  private final ThirdEyeClientMap clientMap;

  public DataCache(ThirdEyeClientMap clientMap) {
    this.clientMap = clientMap;
  }

  public CollectionSchema getCollectionSchema(String serverUri, String collection)
      throws Exception {
    ThirdEyeClient client = clientMap.get(serverUri);
    return CollectionSchema.fromStarTreeConfig(client.getStarTreeConfig(collection));
  }

  public List<String> getCollections(String serverUri) throws Exception {
    return clientMap.get(serverUri).getCollections();
  }

  public List<SegmentDescriptor> getSegmentDescriptors(String serverUri, String collection)
      throws Exception {
    return clientMap.get(serverUri).getSegmentDescriptors(collection);

  }

  public void clear() throws Exception {
    clientMap.clear();
  }

}
