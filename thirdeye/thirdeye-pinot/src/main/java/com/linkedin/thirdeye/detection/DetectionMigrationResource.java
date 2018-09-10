package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection migration resource.
 */
@Path("/migrate")
public class DetectionMigrationResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DetectionMigrationResource.class);

  private final LegacyAnomalyFunctionTranslator translator;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DetectionConfigManager detectionConfigDAO;

  /**
   * Instantiates a new Detection migration resource.
   *
   * @param anomalyFunctionFactory the anomaly function factory
   */
  public DetectionMigrationResource(MetricConfigManager metricConfigDAO,
      AnomalyFunctionManager anomalyFunctionDAO, DetectionConfigManager detectionConfigDAO,
      AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.detectionConfigDAO = detectionConfigDAO;
    this.translator = new LegacyAnomalyFunctionTranslator(metricConfigDAO, anomalyFunctionFactory, alertFilterFactory);
  }

  /**
   * This endpoint takes in a anomaly function Id and translate the anomaly function config to a
   * detection config of the new pipeline and then persist it in to database.
   *
   * @param anomalyFunctionId the anomaly function id
   * @return the response
   * @throws Exception the exception
   */
  @POST
  public Response migrateToDetectionPipeline(
      @QueryParam("id") long anomalyFunctionId,
      @QueryParam("name") String name,
      @QueryParam("lastTimestamp") Long lastTimestamp) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    DetectionConfigDTO config = this.translator.translate(anomalyFunctionDTO);

    if (!StringUtils.isBlank(name)) {
      config.setName(name);
    }

    config.setLastTimestamp(System.currentTimeMillis());
    if (lastTimestamp != null) {
      config.setLastTimestamp(lastTimestamp);
    }

    this.detectionConfigDAO.save(config);
    if (config.getId() == null) {
      throw new WebApplicationException(String.format("Could not migrate anomaly function %d", anomalyFunctionId));
    }

    LOGGER.info("Created detection config {} for anomaly function {}", config.getId(), anomalyFunctionDTO.getId());
    return Response.ok(config.getId()).build();
  }
}
