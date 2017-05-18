package com.linkedin.thirdeye.dashboard.resources;

import java.util.List;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/ingraph-metric-config")
@Produces(MediaType.APPLICATION_JSON)
public class IngraphMetricConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private IngraphMetricConfigManager ingraphMetricConfigDao;

  public IngraphMetricConfigResource() {
    this.ingraphMetricConfigDao = DAO_REGISTRY.getIngraphMetricConfigDAO();
  }

  @GET
  @Path("/create")
  public String createMetricConfig(@QueryParam("rrdName") String rrdName,
      @QueryParam("metricName") String metricName,
      @QueryParam("dashboardName") String dashboardName,
      @QueryParam("metricDataType") String metricDataType,
      @QueryParam("metricSourceType") String metricSourceType,
      @QueryParam("container") String container) {
    try {
      IngraphMetricConfigDTO ingraphMetricConfigDTO = new IngraphMetricConfigDTO();
      ingraphMetricConfigDTO.setRrdName(rrdName);
      ingraphMetricConfigDTO.setMetricName(metricName);
      ingraphMetricConfigDTO.setDashboardName(dashboardName);
      ingraphMetricConfigDTO.setContainer(container);
      ingraphMetricConfigDTO.setMetricDataType(metricDataType);
      ingraphMetricConfigDTO.setMetricSourceType(metricSourceType);

      Long id = ingraphMetricConfigDao.save(ingraphMetricConfigDTO);
      ingraphMetricConfigDTO.setId(id);
      return JsonResponseUtil.buildResponseJSON(ingraphMetricConfigDTO).toString();
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to create rrd:" + rrdName).toString();
    }
  }

  @GET
  @Path("/update")
  public String updateMetricConfig(@NotNull @QueryParam("id") long ingraphMetricConfigId,
      @QueryParam("rrdName") String rrdName,
      @QueryParam("metricName") String metricName,
      @QueryParam("dashboardName") String dashboardName,
      @QueryParam("metricDataType") String metricDataType,
      @QueryParam("metricSourceType") String metricSourceType,
      @QueryParam("container") String container) {
    try {

      IngraphMetricConfigDTO ingraphMetricConfigDTO = ingraphMetricConfigDao.findById(ingraphMetricConfigId);
      ingraphMetricConfigDTO.setRrdName(rrdName);
      ingraphMetricConfigDTO.setMetricName(metricName);
      ingraphMetricConfigDTO.setDashboardName(dashboardName);
      ingraphMetricConfigDTO.setContainer(container);
      ingraphMetricConfigDTO.setMetricDataType(metricDataType);
      ingraphMetricConfigDTO.setMetricSourceType(metricSourceType);

      int numRowsUpdated = ingraphMetricConfigDao.update(ingraphMetricConfigDTO);
      if (numRowsUpdated == 1) {
        return JsonResponseUtil.buildResponseJSON(ingraphMetricConfigDTO).toString();
      } else {
        return JsonResponseUtil.buildErrorResponseJSON("Failed to update metric id:" + ingraphMetricConfigId).toString();
      }
    } catch (Exception e) {
      return JsonResponseUtil.buildErrorResponseJSON("Failed to update metric id:" + ingraphMetricConfigId + ". Exception:" + e.getMessage()).toString();
    }
  }

  @GET
  @Path("/delete")
  public String deleteMetricConfig(@NotNull @QueryParam("dashboardName") String dashboardName, @NotNull @QueryParam("id") Long metricConfigId) {
    ingraphMetricConfigDao.deleteById(metricConfigId);
    return JsonResponseUtil.buildSuccessResponseJSON("Successully deleted " + metricConfigId).toString();
  }

  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewMetricConfig(@NotNull @QueryParam("dashboardName") String dashboardName, @DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("100") @QueryParam("jtPageSize") int jtPageSize) {
    List<IngraphMetricConfigDTO> ingraphMetricConfigDTOs = ingraphMetricConfigDao.findByDashboard(dashboardName);
    List<IngraphMetricConfigDTO> subList = Utils.sublist(ingraphMetricConfigDTOs, jtStartIndex, jtPageSize);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(subList);
    return rootNode.toString();
  }

}
