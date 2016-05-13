package com.linkedin.thirdeye.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;

public class ThirdeyeCacheRegistry {

  private LoadingCache<String,ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String,Schema> schemaCache;
  private LoadingCache<String,CollectionSchema> collectionSchemaCache;
  private LoadingCache<String,Long> collectionMaxDataTimeCache;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdeyeCacheRegistry.class);

  private static ThirdeyeCacheRegistry cacheRegistryInstance = null;

  private ThirdeyeCacheRegistry() {

  }

  public static ThirdeyeCacheRegistry getInstance() {
    if(cacheRegistryInstance == null) {
      LOGGER.info("Creating new ThirdeyeCacheRegistry instance");
      cacheRegistryInstance = new ThirdeyeCacheRegistry();
    }
    return cacheRegistryInstance;
  }

  public LoadingCache<String, ResultSetGroup> getResultSetGroupCache() {
    return resultSetGroupCache;
  }

  public void registerResultSetGroupCache(LoadingCache<String, ResultSetGroup> resultSetGroupCache) {
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
