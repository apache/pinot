package com.linkedin.thirdeye.client;

import java.util.List;

import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.detector.ThirdEyeDetectorConfiguration;

public interface ThirdEyeClient {

  ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  List<String> getCollections() throws Exception;

  /** Clear any cached values. */
  void clear() throws Exception;

  void close() throws Exception;

  CollectionSchema getCollectionSchema(String collection) throws Exception;
  
  long getMaxDataTime(String collection) throws Exception;

}
