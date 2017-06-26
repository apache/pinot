package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class EntityManagerResource {
  private final AnomalyFunctionManager anomalyFunctionManager;
  private final DashboardConfigManager dashboardConfigManager;
  private final MetricConfigManager metricConfigManager;
  private final DatasetConfigManager datasetConfigManager;
  private final OverrideConfigManager overrideConfigManager;
  private final AlertConfigManager alertConfigManager;
  private final ClassificationConfigManager classificationConfigManager;
  private final ApplicationManager applicationManager;
  private final EntityToEntityMappingManager entityToEntityMappingManager;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(EntityManagerResource.class);

  public EntityManagerResource() {
    this.anomalyFunctionManager = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.dashboardConfigManager = DAO_REGISTRY.getDashboardConfigDAO();
    this.metricConfigManager = DAO_REGISTRY.getMetricConfigDAO();
    this.datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();
    this.overrideConfigManager = DAO_REGISTRY.getOverrideConfigDAO();
    this.alertConfigManager = DAO_REGISTRY.getAlertConfigDAO();
    this.classificationConfigManager = DAO_REGISTRY.getClassificationConfigDAO();
    this.applicationManager = DAO_REGISTRY.getApplicationDAO();
    this.entityToEntityMappingManager = DAO_REGISTRY.getEntityToEntityMappingDAO();
  }

  private enum EntityType {
    ANOMALY_FUNCTION, DASHBOARD_CONFIG, DATASET_CONFIG, METRIC_CONFIG,
    OVERRIDE_CONFIG, ALERT_CONFIG, CLASSIFICATION_CONFIG, APPLICATION, ENTITY_MAPPING
  }

  @GET
  public List<EntityType> getAllEntityTypes() {
    return Arrays.asList(EntityType.values());
  }

  @GET
  @Path("{entityType}")
  public Response getAllEntitiesForType(@PathParam("entityType") String entityTypeStr) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    List<AbstractDTO> results = new ArrayList<>();
    switch (entityType) {
    case ANOMALY_FUNCTION:
      results.addAll(anomalyFunctionManager.findAllActiveFunctions());
      break;
    case DASHBOARD_CONFIG:
      results.addAll(dashboardConfigManager.findAll());
      break;
    case DATASET_CONFIG:
      results.addAll(datasetConfigManager.findAll());
      break;
    case METRIC_CONFIG:
      results.addAll(metricConfigManager.findAll());
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
      case ANOMALY_FUNCTION:
        AnomalyFunctionDTO anomalyFunctionDTO = OBJECT_MAPPER.readValue(jsonPayload, AnomalyFunctionDTO.class);
        if (anomalyFunctionDTO.getId() == null) {
          anomalyFunctionManager.save(anomalyFunctionDTO);
        } else {
          anomalyFunctionManager.update(anomalyFunctionDTO);
        }
        break;
      case DASHBOARD_CONFIG:
        DashboardConfigDTO dashboardConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, DashboardConfigDTO.class);
        dashboardConfigManager.update(dashboardConfigDTO);
        break;
      case DATASET_CONFIG:
        DatasetConfigDTO datasetConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, DatasetConfigDTO.class);
        datasetConfigManager.update(datasetConfigDTO);
        break;
      case METRIC_CONFIG:
        MetricConfigDTO metricConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, MetricConfigDTO.class);
        metricConfigManager.update(metricConfigDTO);
        break;
      case OVERRIDE_CONFIG:
        OverrideConfigDTO overrideConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, OverrideConfigDTO.class);
        if (overrideConfigDTO.getId() == null) {
          overrideConfigManager.save(overrideConfigDTO);
        } else {
          overrideConfigManager.update(overrideConfigDTO);
        }
        break;
      case ALERT_CONFIG:
        AlertConfigDTO alertConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, AlertConfigDTO.class);
        if (alertConfigDTO.getId() == null) {
          alertConfigManager.save(alertConfigDTO);
        } else {
          alertConfigManager.update(alertConfigDTO);
        }
        break;
      case CLASSIFICATION_CONFIG:
        ClassificationConfigDTO classificationConfigDTO =
            OBJECT_MAPPER.readValue(jsonPayload, ClassificationConfigDTO.class);
        if (classificationConfigDTO.getId() == null) {
          classificationConfigManager.save(classificationConfigDTO);
        } else {
          classificationConfigManager.update(classificationConfigDTO);
        }
        break;
      case APPLICATION:
        ApplicationDTO applicationDTO = OBJECT_MAPPER.readValue(jsonPayload, ApplicationDTO.class);
        if (applicationDTO.getId() == null) {
          applicationManager.save(applicationDTO);
        } else {
          applicationManager.update(applicationDTO);
        }
        break;
      case ENTITY_MAPPING:
        EntityToEntityMappingDTO mappingDTODTO = OBJECT_MAPPER.readValue(jsonPayload, EntityToEntityMappingDTO.class);
        if (mappingDTODTO.getId() == null) {
          entityToEntityMappingManager.save(mappingDTODTO);
        } else {
          entityToEntityMappingManager.update(mappingDTODTO);
        }
        break;
      }
    } catch (IOException e) {
      LOG.error("Error saving the entity with payload : " + jsonPayload, e);
      throw new WebApplicationException(e);
    }
    return Response.ok().build();
  }
}

