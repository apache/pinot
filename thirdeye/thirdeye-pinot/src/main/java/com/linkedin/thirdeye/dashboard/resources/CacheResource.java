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
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.MetricDataset;

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

    refreshDatasets();

    refreshDatasetConfigCache();
    refreshMetricConfigCache();
    refreshDashoardConfigsCache();

    refreshMaxDataTimeCache();
    refreshDimensionFiltersCache();
    refreshDashboardsCache();

    return Response.ok().build();
  }


  @POST
  @Path("/refresh/maxDataTime")
  public Response refreshMaxDataTimeCache() {
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    LoadingCache<String,Long> cache = CACHE_INSTANCE.getCollectionMaxDataTimeCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/datasetConfig")
  public Response refreshDatasetConfigCache() {
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    LoadingCache<String,DatasetConfigDTO> cache = CACHE_INSTANCE.getDatasetConfigCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/metricConfig")
  public Response refreshMetricConfigCache() {
    LoadingCache<MetricDataset, MetricConfigDTO> cache = CACHE_INSTANCE.getMetricConfigCache();
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    for (String collection : collections) {
      List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(collection);
      for (MetricConfigDTO metricConfig : metricConfigs) {
        cache.refresh(new MetricDataset(metricConfig.getName(), metricConfig.getDataset()));
      }
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/dashboardConfigs")
  public Response refreshDashoardConfigsCache() {
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    LoadingCache<String,List<DashboardConfigDTO>> cache = CACHE_INSTANCE.getDashboardConfigsCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }


  @POST
  @Path("/refresh/filters")
  public Response refreshDimensionFiltersCache() {
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    LoadingCache<String,String> cache = CACHE_INSTANCE.getDimensionFiltersCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/dashboards")
  public Response refreshDashboardsCache() {
    List<String> collections = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    LoadingCache<String,String> cache = CACHE_INSTANCE.getDashboardsCache();
    for (String collection : collections) {
      cache.refresh(collection);
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/collections")
  public Response refreshDatasets() {
    Response response = Response.ok().build();
    CACHE_INSTANCE.getDatasetsCache().loadDatasets();
    return response;
  }

}
