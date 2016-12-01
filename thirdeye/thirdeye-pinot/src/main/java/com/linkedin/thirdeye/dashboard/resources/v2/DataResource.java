package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path(value = "/data")
@Produces(MediaType.APPLICATION_JSON)
public class DataResource {

  //------------- endpoints to fetch summary -------------
  @GET
  @Path("summary/metrics")
  public List<String> getMatricNames(String dataset) {
    return null;
  }

  @GET
  @Path("summary/datasets")
  public List<String> getDatasetNames() {
    return null;
  }

  @GET
  @Path("maxtime/{dataset}")
  public Map<String, Long> getMatricMaxDataTime(@PathParam("dataset") String dataset) {
    return null;
  }

  //------------- endpoints to fetch config objects -------------
  // metric end points
  @GET
  @Path("metrics")
  public List<MetricConfigDTO> getMetrics(
      @QueryParam("pageId") @DefaultValue("0") int pageId,
      @QueryParam("numResults") @DefaultValue("1000") int numResults,
      @QueryParam("dataset") String dataset, @QueryParam("metric") String metric
  ) {
    return new ArrayList<>();
  }

  @GET
  @Path("metric/{id}")
  public MetricConfigDTO getMetricById(@PathParam("id") Long id) {
    return null;
  }


  // dataset end points
  @GET
  @Path("datasets")
  public List<CollectionConfig> getDatasets(
      @QueryParam("pageId") @DefaultValue("0") int pageId,
      @QueryParam("numResults") @DefaultValue("1000") int numResults) {

    return null;
  }

  @GET
  @Path("dataset/{id}")
  public CollectionConfig getDatasetById(@PathParam("id") Long id) {
    return null;
  }

  @GET
  @Path("dataset")
  public CollectionConfig getDatasetByName(@QueryParam("dataset") String dataset) {
    return null;
  }

  @GET
  @Path(value = "filters/{dataset}")
  public Map<String, List<String>> getFilters(@PathParam("dataset") String dataset) {
    return null;
  }

  // dashboard end points
  @GET
  @Path("dashboard")
  public List<DashboardConfig> getDashboards(@QueryParam("dataset") String dataset) {
    return new ArrayList<>();
  }

  @GET
  @Path("dashboard/{id}")
  public DashboardConfig getDashboardById(@PathParam("id") Long id) {
    return null;
  }

}
