package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

@Path("/cache")
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheResource.class);
  private ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public String getCaches() {
    return "usage";
  }

  @POST
  @Path("/refresh")
  public Response refreshAllCaches() {
    try {
    CACHE_INSTANCE.getCollectionsCache().loadCollections();

    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getCollectionSchemaCache().refresh(collection);
      CACHE_INSTANCE.getSchemaCache().refresh(collection);
      CACHE_INSTANCE.getCollectionMaxDataTimeCache().refresh(collection);
      CACHE_INSTANCE.getCollectionConfigCache().refresh(collection);
      CACHE_INSTANCE.getDimensionFiltersCache().refresh(collection);
      CACHE_INSTANCE.getDashboardsCache().refresh(collection);
    }
    } catch (Exception e) {
      LOGGER.error("Exception while refresing caches", e);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/schema")
  public Response refreshSchemaCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String,Schema> cache = CACHE_INSTANCE.getSchemaCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collectionSchema")
  public Response refreshCollectionSchemaCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String,CollectionSchema> cache = CACHE_INSTANCE.getCollectionSchemaCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/maxDataTime")
  public Response refreshMaxDataTimeCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String,Long> cache = CACHE_INSTANCE.getCollectionMaxDataTimeCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collectionConfig")
  public Response refreshCollectionConfigCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String, CollectionConfig> collectionConfigCache = CACHE_INSTANCE.getCollectionConfigCache();
    LoadingCache<String, String> collectionAliasCache = CACHE_INSTANCE.getCollectionAliasCache();
    collectionAliasCache.invalidateAll();
    for (String collection : collections) {

      try {
        collectionConfigCache.refresh(collection);

        // refresh aliases
        CollectionConfig collectionConfig = collectionConfigCache.get(collection);
        String collectionAlias = collectionConfig.getCollectionAlias();
        if (StringUtils.isNotEmpty(collectionAlias)) {
          collectionAliasCache.refresh(collectionAlias);
        }
      } catch (InvalidCacheLoadException | ExecutionException e) {
        LOGGER.debug("No CollectionConfig for collection", collection);
      }
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/filters")
  public Response refreshDimensionFiltersCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String,String> cache = CACHE_INSTANCE.getDimensionFiltersCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/dashboards")
  public Response refreshDashboardsCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String,String> cache = CACHE_INSTANCE.getDashboardsCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collections")
  public Response refreshCollections() {
    Response response = Response.ok().build();
    try {
      CACHE_INSTANCE.getCollectionsCache().loadCollections();
    } catch (IOException e) {
      LOGGER.error("Exception while refreshing collections cache", e);
      response = Response.serverError().build();
    }
    return response;
  }

}
