package com.linkedin.thirdeye.dashboard.resources;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.MetricDataset;

@Path("/cache")
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {

  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(20);
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

    refreshMaxDataTimeCache();
    refreshDimensionFiltersCache();

    return Response.ok().build();
  }


  @POST
  @Path("/refresh/maxDataTime")
  public Response refreshMaxDataTimeCache() {
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,Long> cache = CACHE_INSTANCE.getDatasetMaxDataTimeCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/datasetConfig")
  public Response refreshDatasetConfigCache() {
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,DatasetConfigDTO> cache = CACHE_INSTANCE.getDatasetConfigCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/metricConfig")
  public Response refreshMetricConfigCache() {
    final LoadingCache<MetricDataset, MetricConfigDTO> cache = CACHE_INSTANCE.getMetricConfigCache();
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
          for (MetricConfigDTO metricConfig : metricConfigs) {
            cache.refresh(new MetricDataset(metricConfig.getName(), metricConfig.getDataset()));
          }

        }
      });
    }
    return Response.ok().build();
  }


  @POST
  @Path("/refresh/filters")
  public Response refreshDimensionFiltersCache() {
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,String> cache = CACHE_INSTANCE.getDimensionFiltersCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
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
