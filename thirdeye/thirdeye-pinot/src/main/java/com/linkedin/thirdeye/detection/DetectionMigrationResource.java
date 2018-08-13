package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;


@Path("/migrate")
public class DetectionMigrationResource {
  private final LegacyAnomalyFunctionTranslator translator;
  private final AnomalyFunctionManager anomalyFunctionDAO;

  public DetectionMigrationResource(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
    this.translator = new LegacyAnomalyFunctionTranslator(anomalyFunctionFactory);
  }

  @POST
  public Response migrateToDetectionPipeline(@QueryParam("id") long anomalyFunctionId) {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    this.translator.translate(anomalyFunctionDTO);
    return Response.ok().build();
  }
}
