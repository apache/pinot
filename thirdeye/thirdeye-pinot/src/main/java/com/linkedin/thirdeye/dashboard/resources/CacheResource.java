package com.linkedin.thirdeye.dashboard.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.MetricDataset;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

@Path("/cache")
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheResource.class);
  private ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

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
      CACHE_INSTANCE.getCollectionMaxDataTimeCache().refresh(collection);
      CACHE_INSTANCE.getDimensionFiltersCache().refresh(collection);
      CACHE_INSTANCE.getDashboardsCache().refresh(collection);
    }
    } catch (Exception e) {
      LOGGER.error("Exception while refresing caches", e);
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
  @Path("/refresh/datasetConfig")
  public Response refreshDatasetConfigCache() {
    List<DatasetConfigDTO> datasetConfigs = DAO_REGISTRY.getDatasetConfigDAO().findAll();
    LoadingCache<String,DatasetConfigDTO> cache = CACHE_INSTANCE.getDatasetConfigCache();
    for (DatasetConfigDTO datasetConfig : datasetConfigs) {
      cache.refresh(datasetConfig.getDataset());
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/metricConfig")
  public Response refreshMetricConfigCache() {
    List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findAll();
    LoadingCache<MetricDataset, MetricConfigDTO> cache = CACHE_INSTANCE.getMetricConfigCache();
    for (MetricConfigDTO metricConfig : metricConfigs) {
      cache.refresh(new MetricDataset(metricConfig.getName(), metricConfig.getDataset()));
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/dashboardConfigs")
  public Response refreshDashoardConfigsCache() {
    List<DatasetConfigDTO> datasetConfigs = DAO_REGISTRY.getDatasetConfigDAO().findAll();
    LoadingCache<String,List<DashboardConfigDTO>> cache = CACHE_INSTANCE.getDashboardConfigsCache();
    for (DatasetConfigDTO datasetConfig : datasetConfigs) {
      cache.refresh(datasetConfig.getDataset());
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
    CACHE_INSTANCE.getCollectionsCache().loadCollections();
    return response;
  }

}
