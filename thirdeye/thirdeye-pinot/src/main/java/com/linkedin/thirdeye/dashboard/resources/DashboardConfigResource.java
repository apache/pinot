package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

@Path(value = "/thirdeye-admin/dashboard-config")
@Produces(MediaType.APPLICATION_JSON)
public class DashboardConfigResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(DashboardConfigResource.class);

  private DashboardConfigManager dashboardConfigDAO;

  public DashboardConfigResource() {
    this.dashboardConfigDAO = DAO_REGISTRY.getDashboardConfigDAO();
  }


  @DELETE
  @Path("/delete")
  public void deleteDashboardConfig(@NotNull @QueryParam("id") Long dashboardConfigId) {
    dashboardConfigDAO.deleteById(dashboardConfigId);
  }

  @GET
  @Path("/view")
  @Produces(MediaType.APPLICATION_JSON)
  public List<DashboardConfigDTO> viewAll() {
    return dashboardConfigDAO.findAll();
  }

  @GET
  @Path("/view/{dataset}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<DashboardConfigDTO> viewByDataset(@PathParam("dataset") String dataset) {
    List<DashboardConfigDTO> dashboardConfigs = dashboardConfigDAO.findByDataset(dataset);
    return dashboardConfigs;
  }


  @POST
  @Path("/create/payload")
  public Long createDashboardConfig(@QueryParam("payload") String payload) {
    Long id = null;
    try {
      DashboardConfigDTO dashboardConfig = OBJECT_MAPPER.readValue(payload, DashboardConfigDTO.class);
      id = dashboardConfigDAO.save(dashboardConfig);
    } catch (IOException e) {
      LOG.error("Exception in creating dashboard config with payload {}", payload);
    }
    return id;
  }

  public Long createDashboardConfig(DashboardConfigDTO dashboardConfig) {
    Long id = dashboardConfigDAO.save(dashboardConfig);
    return id;
  }

  public void updateDashboardConfig(DashboardConfigDTO dashboardConfig) {
    dashboardConfigDAO.update(dashboardConfig);
  }

}
