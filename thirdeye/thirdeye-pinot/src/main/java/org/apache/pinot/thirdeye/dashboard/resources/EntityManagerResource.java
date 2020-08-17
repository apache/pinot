/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources;

import static org.apache.pinot.thirdeye.dashboard.resources.ResourceUtils.badRequest;
import static org.apache.pinot.thirdeye.dashboard.resources.ResourceUtils.ensure;
import static org.apache.pinot.thirdeye.dashboard.resources.ResourceUtils.ensureExists;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ClassificationConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.OverrideConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ClassificationConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OverrideConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DatasetConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.SessionBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.Arrays;
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


@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DASHBOARD_TAG})
@Singleton
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

  @Inject
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
    ensureExists(entityTypeStr, "entityType is null");

    final EntityType entityType = entityType(entityTypeStr);

    switch (entityType) {
      case ANOMALY_FUNCTION:
        return anomalyFunctionManager.findAll();

      case DATASET_CONFIG:
        List<DatasetConfigDTO> allDatasets = datasetConfigManager.findAll();
        allDatasets.sort(Comparator.comparing(DatasetConfigBean::getDataset));
        return allDatasets;

      case METRIC_CONFIG:
        List<MetricConfigDTO> allMetrics = metricConfigManager.findAll();
        allMetrics.sort(Comparator.comparing(MetricConfigBean::getDataset));
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
    ensure(!Strings.isNullOrEmpty(entityTypeStr), "entityType must be non-empty");

    final EntityType entityType = entityType(entityTypeStr);

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
          throw badRequest("Entity type not supported: " + entityType);
      }

    } catch (IOException e) {
      LOG.error("Error saving the entity with payload : " + jsonPayload, e);
      throw new WebApplicationException(e);
    }
  }

  private static EntityType entityType(final String entityTypeStr) {
    try {
      return EntityType.valueOf(entityTypeStr);
    } catch (IllegalArgumentException e) {
      throw badRequest("Invalid type: " + entityTypeStr);
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

