/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("thirdeye/entity")
@Produces(MediaType.APPLICATION_JSON)
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
  public List<? extends AbstractDTO> getAllEntitiesForType(@PathParam("entityType") String entityTypeStr) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);

    switch (entityType) {
      case ANOMALY_FUNCTION:
        return anomalyFunctionManager.findAll();

      case DATASET_CONFIG:
        List<DatasetConfigDTO> allDatasets = datasetConfigManager.findAll();
        Collections.sort(allDatasets, new Comparator<DatasetConfigDTO>() {

          @Override
          public int compare(DatasetConfigDTO o1, DatasetConfigDTO o2) {
            return o1.getDataset().compareTo(o2.getDataset());
          }
        });
        return allDatasets;

      case METRIC_CONFIG:
        List<MetricConfigDTO> allMetrics = metricConfigManager.findAll();
        Collections.sort(allMetrics, new Comparator<MetricConfigDTO>() {

          @Override
          public int compare(MetricConfigDTO o1, MetricConfigDTO o2) {
            return o1.getDataset().compareTo(o2.getDataset());
          }
        });
        return allMetrics;

      case OVERRIDE_CONFIG:
        return overrideConfigManager.findAll();

      case ALERT_CONFIG:
        return alertConfigManager.findAll();

      case CLASSIFICATION_CONFIG:
        return classificationConfigManager.findAll();

      case APPLICATION:
        return applicationManager.findAll();

      case ENTITY_MAPPING:
        return entityToEntityMappingManager.findAll();

      case DETECTION_CONFIG:
        return detectionConfigManager.findAll();

      case MERGED_ANOMALY:
        return mergedAnomalyResultManager.findByPredicate(Predicate.GT("detectionConfigId", 0));

      case SESSION:
        return sessionManager.findByPredicate(Predicate.EQ("principalType", SessionBean.PrincipalType.SERVICE));

      case DETECTION_ALERT_CONFIG:
        return detectionAlertConfigManager.findAll();

      default:
        throw new WebApplicationException("Unknown entity type : " + entityType);
    }
  }

  @POST
  public Long updateEntity(@QueryParam("entityType") String entityTypeStr, String jsonPayload) {
    if (Strings.isNullOrEmpty(entityTypeStr)) {
      throw new WebApplicationException("EntryType can not be null");
    }

    EntityType entityType = EntityType.valueOf(entityTypeStr);

    try {
      switch (entityType) {
        case DATASET_CONFIG:
          return assertNotNull(datasetConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, DatasetConfigDTO.class)));

        case METRIC_CONFIG:
          return assertNotNull(metricConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, MetricConfigDTO.class)));

        case ANOMALY_FUNCTION:
          return assertNotNull(anomalyFunctionManager.save(OBJECT_MAPPER.readValue(jsonPayload, AnomalyFunctionDTO.class)));

        case OVERRIDE_CONFIG:
          return assertNotNull(overrideConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, OverrideConfigDTO.class)));

        case ALERT_CONFIG:
          AlertConfigDTO alertConfigDTO = OBJECT_MAPPER.readValue(jsonPayload, AlertConfigDTO.class);
          if (Strings.isNullOrEmpty(alertConfigDTO.getFromAddress())) {
            alertConfigDTO.setFromAddress(config.getFailureToAddress());
          }

          return assertNotNull(alertConfigManager.save(alertConfigDTO));

        case CLASSIFICATION_CONFIG:
          return assertNotNull(classificationConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, ClassificationConfigDTO.class)));

        case APPLICATION:
          return assertNotNull(applicationManager.save(OBJECT_MAPPER.readValue(jsonPayload, ApplicationDTO.class)));

        case ENTITY_MAPPING:
          return assertNotNull(entityToEntityMappingManager.save(OBJECT_MAPPER.readValue(jsonPayload, EntityToEntityMappingDTO.class)));

        case DETECTION_CONFIG:
          return assertNotNull(detectionConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, DetectionConfigDTO.class)));

        case MERGED_ANOMALY:
          return assertNotNull(mergedAnomalyResultManager.save(OBJECT_MAPPER.readValue(jsonPayload, MergedAnomalyResultDTO.class)));

        case SESSION:
          return assertNotNull(sessionManager.save(OBJECT_MAPPER.readValue(jsonPayload, SessionDTO.class)));

        case DETECTION_ALERT_CONFIG:
          return assertNotNull(detectionAlertConfigManager.save(OBJECT_MAPPER.readValue(jsonPayload, DetectionAlertConfigDTO.class)));

        default:
          throw new WebApplicationException("Unknown entity type : " + entityType);
      }

    } catch (IOException e) {
      LOG.error("Error saving the entity with payload : " + jsonPayload, e);
      throw new WebApplicationException(e);
    }
  }

  private static Long assertNotNull(Long id) {
    if (id == null) {
      throw new WebApplicationException("Could not save entity");
    }
    return id;
  }

  // TODO: create a common delete end point for these entities
}

