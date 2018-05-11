package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.SessionManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.SessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.SessionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("thirdeye/entity")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DASHBOARD_TAG})
public class EntityManagerResource {
  private final AnomalyFunctionManager anomalyFunctionManager;
  private final MetricConfigManager metricConfigManager;
  private final DatasetConfigManager datasetConfigManager;
  private final OverrideConfigManager overrideConfigManager;
  private final AlertConfigManager alertConfigManager;
  private final ClassificationConfigManager classificationConfigManager;
  private final ApplicationManager applicationManager;
  private final EntityToEntityMappingManager entityToEntityMappingManager;
  private final SessionManager sessionManager;
  private final DetectionConfigManager detectionConfigManager;
  private final MergedAnomalyResultManager mergedAnomalyResultManager;
  private final DetectionAlertConfigManager detectionAlertConfigManager;
  private final ThirdEyeConfiguration config;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(EntityManagerResource.class);

  public EntityManagerResource(ThirdEyeConfiguration configuration) {
    this.anomalyFunctionManager = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.metricConfigManager = DAO_REGISTRY.getMetricConfigDAO();
    this.datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();
    this.overrideConfigManager = DAO_REGISTRY.getOverrideConfigDAO();
    this.alertConfigManager = DAO_REGISTRY.getAlertConfigDAO();
    this.classificationConfigManager = DAO_REGISTRY.getClassificationConfigDAO();
    this.applicationManager = DAO_REGISTRY.getApplicationDAO();
    this.entityToEntityMappingManager = DAO_REGISTRY.getEntityToEntityMappingDAO();
    this.sessionManager = DAO_REGISTRY.getSessionDAO();
    this.detectionConfigManager = DAO_REGISTRY.getDetectionConfigManager();
    this.mergedAnomalyResultManager = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.detectionAlertConfigManager = DAO_REGISTRY.getDetectionAlertConfigManager();
    this.config = configuration;
  }

  private enum EntityType {
    ANOMALY_FUNCTION,
    DATASET_CONFIG,
    METRIC_CONFIG,
    OVERRIDE_CONFIG,
    ALERT_CONFIG,
    CLASSIFICATION_CONFIG,
    APPLICATION,
    ENTITY_MAPPING,
    DETECTION_CONFIG,
    DETECTION_ALERT_CONFIG,
    MERGED_ANOMALY,
    SESSION
  }

  @GET
  public List<EntityType> getAllEntityTypes() {
    return Arrays.asList(EntityType.values());
  }

  @GET
  @Path("{entityType}")
  @ApiOperation(value = "GET request to get all entities for a type.")
  public Response getAllEntitiesForType(@PathParam("entityType") String entityTypeStr) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    List<AbstractDTO> results = new ArrayList<>();
    switch (entityType) {
      case ANOMALY_FUNCTION:
        results.addAll(anomalyFunctionManager.findAll());
        break;
      case DATASET_CONFIG:
        List<DatasetConfigDTO> allDatasets = datasetConfigManager.findAll();
        Collections.sort(allDatasets, new Comparator<DatasetConfigDTO>() {

          @Override
          public int compare(DatasetConfigDTO o1, DatasetConfigDTO o2) {
            return o1.getDataset().compareTo(o2.getDataset());
          }
        });
        results.addAll(allDatasets);
        break;
      case METRIC_CONFIG:
        List<MetricConfigDTO> allMetrics = metricConfigManager.findAll();
        Collections.sort(allMetrics, new Comparator<MetricConfigDTO>() {

          @Override
          public int compare(MetricConfigDTO o1, MetricConfigDTO o2) {
            return o1.getDataset().compareTo(o2.getDataset());
          }
        });
        results.addAll(allMetrics);
        break;
      case OVERRIDE_CONFIG:
        results.addAll(overrideConfigManager.findAll());
        break;
      case ALERT_CONFIG:
        results.addAll(alertConfigManager.findAll());
        break;
      case CLASSIFICATION_CONFIG:
        results.addAll(classificationConfigManager.findAll());
        break;
      case APPLICATION:
        results.addAll(applicationManager.findAll());
        break;
      case ENTITY_MAPPING:
        results.addAll(entityToEntityMappingManager.findAll());
        break;
      case DETECTION_CONFIG:
        results.addAll(detectionConfigManager.findAll());
        break;
      case MERGED_ANOMALY:
        results.addAll(mergedAnomalyResultManager.findByPredicate(Predicate.GT("detectionConfigId", 0)));
        break;
      case SESSION:
        results.addAll(sessionManager.findByPredicate(Predicate.EQ("principalType", SessionBean.PrincipalType.SERVICE)));
        break;
      case DETECTION_ALERT_CONFIG:
        results.addAll(detectionAlertConfigManager.findAll());
        break;
      default:
        throw new WebApplicationException("Unknown entity type : " + entityType);
    }
    return Response.ok(results).build();
  }

  @POST
  public Response updateEntity(@QueryParam("entityType") String entityTypeStr, String jsonPayload) {
    if (Strings.isNullOrEmpty(entityTypeStr)) {
      throw new WebApplicationException("EntryType can not be null");
    }
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    try {
      switch (entityType) {
        case DATASET_CONFIG:
          datasetConfigManager.update(OBJECT_MAPPER.readValue(jsonPayload, DatasetConfigDTO.class));
          break;

        case METRIC_CONFIG:
          metricConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, MetricConfigDTO.class));
          break;

        case ANOMALY_FUNCTION:
          anomalyFunctionManager.save(OBJECT_MAPPER.readValue(jsonPayload, AnomalyFunctionDTO.class));
          break;

        case OVERRIDE_CONFIG:
          overrideConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, OverrideConfigDTO.class));
          break;

        case ALERT_CONFIG:
          AlertConfigDTO alertConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, AlertConfigDTO.class);
          if (Strings.isNullOrEmpty(alertConfigDTO.getFromAddress())) {
            alertConfigDTO.setFromAddress(config.getFailureToAddress());
          }
          alertConfigManager.save(alertConfigDTO);
          break;

        case CLASSIFICATION_CONFIG:
          classificationConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, ClassificationConfigDTO.class));
          break;

        case APPLICATION:
          applicationManager.save(OBJECT_MAPPER.readValue(jsonPayload, ApplicationDTO.class));
          break;

        case ENTITY_MAPPING:
          entityToEntityMappingManager.save(OBJECT_MAPPER.readValue(jsonPayload, EntityToEntityMappingDTO.class));
          break;

        case DETECTION_CONFIG:
          detectionConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, DetectionConfigDTO.class));
          break;

        case MERGED_ANOMALY:
          mergedAnomalyResultManager.save(OBJECT_MAPPER.readValue(jsonPayload, MergedAnomalyResultDTO.class));
          break;

        case SESSION:
          sessionManager.save(OBJECT_MAPPER.readValue(jsonPayload, SessionDTO.class));
          break;

        case DETECTION_ALERT_CONFIG:
          detectionAlertConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, DetectionAlertConfigDTO.class));
          break;

        default:
          throw new WebApplicationException("Unknown entity type : " + entityType);
      }
    } catch (IOException e) {
      LOG.error("Error saving the entity with payload : " + jsonPayload, e);
      throw new WebApplicationException(e);
    }
    return Response.ok().build();
  }

  // TODO: create a common delete end point for these entities
}

