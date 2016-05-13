package com.linkedin.thirdeye.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;

public class ThirdeyeCacheRegistry {

  private LoadingCache<PinotQuery,ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String,Schema> schemaCache;
  private LoadingCache<String,CollectionSchema> collectionSchemaCache;
  private LoadingCache<String,Long> collectionMaxDataTimeCache;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdeyeCacheRegistry.class);


  private static class Holder {
    static final ThirdeyeCacheRegistry INSTANCE = new ThirdeyeCacheRegistry();
  }

  public static ThirdeyeCacheRegistry getInstance() {
      return Holder.INSTANCE;
  }

  public LoadingCache<PinotQuery, ResultSetGroup> getResultSetGroupCache() {
    return resultSetGroupCache;
  }

  public void registerResultSetGroupCache(LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache) {
    this.resultSetGroupCache = resultSetGroupCache;
  }

  public LoadingCache<String, Schema> getSchemaCache() {
    return schemaCache;
  }

  public void registerSchemaCache(LoadingCache<String, Schema> schemaCache) {
    this.schemaCache = schemaCache;
  }

  public LoadingCache<String, CollectionSchema> getCollectionSchemaCache() {
    return collectionSchemaCache;
  }

  public void registerCollectionSchemaCache(LoadingCache<String, CollectionSchema> collectionSchemaCache) {
    this.collectionSchemaCache = collectionSchemaCache;
  }

  public LoadingCache<String, Long> getCollectionMaxDataTimeCache() {
    return collectionMaxDataTimeCache;
  }

  public void registerCollectionMaxDataTimeCache(LoadingCache<String, Long> collectionMaxDataTimeCache) {
    this.collectionMaxDataTimeCache = collectionMaxDataTimeCache;
  }

}
