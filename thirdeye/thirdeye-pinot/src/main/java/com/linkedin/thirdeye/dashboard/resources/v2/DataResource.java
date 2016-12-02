package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
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
import org.apache.commons.lang3.StringUtils;

/**
 * Do's and Dont's
 * ================
 * 1. Prefer PathParams over QueryParams
 * 2. Protocols : use Post for new entity creation, Put for update, Delete for delete and Get for retrieval
 * 3. Dont use OBJECT_MAPPER unnecessarily as REST library already takes care of marshalling your object to JSON
 *
 * 4. Errors: there are few ways to handle server side errors
 *    a. catch exception and throw as WebApplicationException : its a REST library exception, you can pass your error response etc into this exception
 *    b. Add a ExceptionMapper and register it in the dw environment
 *    c. Add a web filter / intercepter to catch and convert RTEs to web exception
 */
@Path(value = "/data")
@Produces(MediaType.APPLICATION_JSON)
public class DataResource {
  public static final DAORegistry daoRegistry = DAORegistry.getInstance();

  private final MetricConfigManager metricConfigManager;
  private final DatasetConfigManager datasetConfigManager;
  private final DashboardConfigManager dashboardConfigManager;

  public DataResource() {
    metricConfigManager = daoRegistry.getMetricConfigDAO();
    datasetConfigManager = daoRegistry.getDatasetConfigDAO();
    dashboardConfigManager = daoRegistry.getDashboardConfigDAO();
  }

  //------------- endpoints to fetch summary -------------
  @GET
  @Path("summary/metrics/{dataset}")
  public List<String> getMatricNames(@PathParam("dataset") String dataset) {
      List<MetricConfigDTO> metrics = metricConfigManager.findActiveByDataset(dataset);
      List<String> metricsNames = new ArrayList<>();
      for (MetricConfigDTO metricConfigDTO : metrics) {
        metricsNames.add(metricConfigDTO.getName());
      }
      return metricsNames;
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
    // TODO: add pagination support through out the data managers
    List<MetricConfigDTO> output = new ArrayList<>();
    if (StringUtils.isEmpty(dataset)) {
      output.addAll(metricConfigManager.findAll());
    } else {
      if (StringUtils.isNotEmpty(metric)) {
        output.addAll(metricConfigManager.findActiveByDataset(dataset));
      } else {
        output.add(metricConfigManager.findByMetricAndDataset(metric, dataset));
      }
    }
    return output;
  }

  @GET
  @Path("metric/{id}")
  public MetricConfigDTO getMetricById(@PathParam("id") Long id) {
    return metricConfigManager.findById(id);
  }

  // dataset end points
  @GET
  @Path("datasets")
  public List<DatasetConfigDTO> getDatasets(
      @QueryParam("pageId") @DefaultValue("0") int pageId,
      @QueryParam("numResults") @DefaultValue("1000") int numResults) {

    return null;
  }

  @GET
  @Path("dataset/{id}")
  public DatasetConfigDTO getDatasetById(@PathParam("id") Long id) {
    return datasetConfigManager.findById(id);
  }

  @GET
  @Path("dataset")
  public DatasetConfigDTO getDatasetByName(@QueryParam("dataset") String dataset) {
    return datasetConfigManager.findByDataset(dataset);
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
