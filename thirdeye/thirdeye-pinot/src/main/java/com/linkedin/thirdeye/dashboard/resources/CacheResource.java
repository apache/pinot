package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdeyeCacheRegistry;

@Path("/cache")
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheResource.class);
  private ThirdeyeCacheRegistry CACHE_INSTANCE = ThirdeyeCacheRegistry.getInstance();

  public CacheResource() {

  }

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public String getCaches() {
    return "usage";
  }

  @POST
  @Path("/refresh")
  public Response refreshAllCaches() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getCollectionSchemaCache().refresh(collection);
      CACHE_INSTANCE.getSchemaCache().refresh(collection);
      CACHE_INSTANCE.getCollectionMaxDataTimeCache().refresh(collection);
      CACHE_INSTANCE.getCollectionConfigCache().refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/schema")
  public Response refreshSchemaCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getSchemaCache().refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collectionSchema")
  public Response refreshCollectionSchemaCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getCollectionSchemaCache().refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/maxDataTime")
  public Response refreshMaxDataTimeCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getCollectionMaxDataTimeCache().refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collectionConfig")
  public Response refreshCollectionConfigCache() {
    List<String> collections = CACHE_INSTANCE.getCollectionsCache().getCollections();
    for (String collection : collections) {
      CACHE_INSTANCE.getCollectionConfigCache().refresh(collection);
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
