package com.linkedin.thirdeye.dataset;

import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@Path("/dataset-auto-onboard")
@Produces(MediaType.APPLICATION_JSON)
public class DataSetAutoOnboardResource {
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private final MetricConfigManager metricDAO;

  public DataSetAutoOnboardResource() {
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
  }

  @GET
  @Path("/metrics")
  public Response detectionPreview(@QueryParam("dataset") String dataSet) throws Exception {
    List<MetricConfigDTO> metrics = this.metricDAO.findByDataset(dataSet);
    return Response.ok(metrics).build();
  }

}

