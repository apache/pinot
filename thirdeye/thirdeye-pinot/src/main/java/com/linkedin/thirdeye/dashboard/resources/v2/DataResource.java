package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.resources.v2.dashboard.DashboardSummary;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.Collections;
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

  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final DashboardConfigManager dashboardConfigDAO;

  public DataResource() {
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();
  }

  //------------- endpoints to fetch summary -------------
  @GET
  @Path("summary/metrics")
  public List<String> getMetricNamesForDataset(@QueryParam("dataset") String dataset) {
    List<MetricConfigDTO> metrics = new ArrayList<>();
    if (Strings.isNullOrEmpty(dataset)) {
      metrics.addAll(metricConfigDAO.findAll());
    } else {
      metrics.addAll(metricConfigDAO.findActiveByDataset(dataset));
    }
    List<String> metricsNames = new ArrayList<>();
    for (MetricConfigDTO metricConfigDTO : metrics) {
      metricsNames.add(metricConfigDTO.getName());
    }
    return metricsNames;
  }

  @GET
  @Path("summary/dashboards")
  public List<String> getDashboardNames() {
    List<String> output = new ArrayList<>();
    List<DashboardConfigDTO> dashboardConfigDTOs = dashboardConfigDAO.findAll();
    for (DashboardConfigDTO dashboardConfigDTO : dashboardConfigDTOs) {
      output.add(dashboardConfigDTO.getName());
    }
    return output;
  }

  @GET
  @Path("summary/datasets")
  public List<String> getDatasetNames() {
    List<String> output = new ArrayList<>();
    List<DatasetConfigDTO> datasetConfigDTOs = datasetConfigDAO.findAll();
    for (DatasetConfigDTO dto : datasetConfigDTOs) {
      output.add(dto.getDataset());
    }
    return output;
  }

  @GET
  @Path("maxtime/{dataset}")
  public Map<String, Long> getMetricMaxDataTime(@PathParam("dataset") String dataset) {
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
      output.addAll(metricConfigDAO.findAll());
    } else {
      if (StringUtils.isNotEmpty(metric)) {
        output.addAll(metricConfigDAO.findActiveByDataset(dataset));
      } else {
        output.add(metricConfigDAO.findByMetricAndDataset(metric, dataset));
      }
    }
    return output;
  }

  @GET
  @Path("metric/{id}")
  public MetricConfigDTO getMetricById(@PathParam("id") Long id) {
    return metricConfigDAO.findById(id);
  }


  @GET
  @Path("metric/aliases")
  public List<String> getMetricAliasesWhereNameLike(@QueryParam("name") String name) {
    List<String> metricConfigs = metricConfigDAO.findMetricAliasWhereNameLike("%" + name + "%");
    return metricConfigs;
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
    return datasetConfigDAO.findById(id);
  }

  @GET
  @Path("dataset")
  public DatasetConfigDTO getDatasetByName(@QueryParam("dataset") String dataset) {
    return datasetConfigDAO.findByDataset(dataset);
  }

  @GET
  @Path(value = "filters/{dataset}")
  public Map<String, List<String>> getFilters(@PathParam("dataset") String dataset) {
    return null;
  }

  // dashboard end points
  @GET
  @Path("dashboard")
  public List<DashboardConfig> getDashboards(@QueryParam("dashboard") String dashboard) {
    return new ArrayList<>();
  }

  @GET
  @Path("autocomplete/dashboard")
  public List<DashboardConfigDTO> getDashboardAliasesWhereNameLike(@QueryParam("name") String name) {
    return dashboardConfigDAO.findWhereNameLike("%" + name + "%");
  }

  @GET
  @Path("dashboard/{id}")
  public DashboardConfig getDashboardById(@PathParam("id") Long id) {
    return null;
  }

  @GET
  @Path("dashboard/metricids")
  public List<Long> getMetricIdsByDashboard(@QueryParam("name") String name) {
    if (StringUtils.isBlank(name)) {
      return Collections.emptyList();
    }
    DashboardConfigDTO dashboard = dashboardConfigDAO.findByName(name);
    return dashboard.getMetricIds();
  }

  @GET
  @Path("dashboard/wowsummary")
  public DashboardSummary getWoWSummary(@QueryParam("name") String name) {
    return null;
  }
}
