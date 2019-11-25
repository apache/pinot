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

package org.apache.pinot.thirdeye.detection.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.security.PermitAll;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.dashboard.DetectionPreviewConfiguration;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.onboard.YamlOnboardingTaskInfo;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.validators.SubscriptionConfigValidator;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;
import static org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator.*;


@Path("/yaml")
@Api(tags = {Constants.YAML_TAG})
public class YamlResource {
  protected static final Logger LOG = LoggerFactory.getLogger(YamlResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private enum YamlOperations {
    CREATING, UPDATING, PREVIEW, RUNNING
  }

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_SUBSCRIPTION = "subscription";

  private static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  private static final String PROP_DETECTION_NAME = "detectionName";

  private static final String PROP_SESSION_KEY = "sessionKey";
  private static final String PROP_PRINCIPAL_TYPE = "principalType";
  private static final String PROP_SERVICE = "SERVICE";

  // default onboarding replay period
  private static final long ONBOARDING_REPLAY_LOOKBACK = TimeUnit.DAYS.toMillis(30);

  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final DetectionConfigValidator detectionValidator;
  private final SubscriptionConfigValidator subscriptionValidator;
  private final DataProvider provider;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final TaskManager taskDAO;
  private final SessionManager sessionDAO;
  private final DetectionPipelineLoader loader;
  private final Yaml yaml;
  private final ExecutorService executor;
  private final long previewTimeout;
  private final DetectionConfigFormatter detectionConfigFormatter;

  public YamlResource(DetectionPreviewConfiguration previewConfig) {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.sessionDAO = DAORegistry.getInstance().getSessionDAO();
    this.yaml = new Yaml();
    this.executor = Executors.newFixedThreadPool(previewConfig.getParallelism());
    this.previewTimeout = previewConfig.getTimeout();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO, timeseriesLoader, aggregationLoader, loader);

    this.detectionValidator = new DetectionConfigValidator(this.provider);
    this.subscriptionValidator = new SubscriptionConfigValidator();
    this.detectionConfigFormatter = new DetectionConfigFormatter(metricDAO, datasetDAO);
  }

  /*
   * Helper method to build the detection config from a yaml.
   */
  private DetectionConfigDTO buildDetectionConfigFromYaml(long tuningStartTime, long tuningEndTime, String yamlConfig,
      DetectionConfigDTO existingConfig) {

    // Configure the tuning window
    if (tuningStartTime == 0L && tuningEndTime == 0L) {
      // default tuning window 28 days
      tuningEndTime = System.currentTimeMillis();
      tuningStartTime = tuningEndTime - TimeUnit.DAYS.toMillis(28);
    }

    // Translate the raw yaml config to detection config object
    DetectionConfigDTO config = new DetectionConfigTranslator(yamlConfig, this.provider).translate();

    if (existingConfig != null) {
      config.setId(existingConfig.getId());
      config.setLastTimestamp(existingConfig.getLastTimestamp());
      config.setCreatedBy(existingConfig.getCreatedBy());
    }

    // Tune the detection config - Passes the raw yaml params & injects tuned params
    DetectionConfigTuner detectionTuner = new DetectionConfigTuner(config, provider);
    config = detectionTuner.tune(tuningStartTime, tuningEndTime);
    this.detectionValidator.validateConfig(config);
    return config;
  }

  /*
   * Create a yaml onboarding task. It runs 1 month replay and re-tune the pipeline.
   */
  private void createYamlOnboardingTask(long configId, long tuningWindowStart, long tuningWindowEnd) throws Exception {
    YamlOnboardingTaskInfo info = new YamlOnboardingTaskInfo();
    info.setConfigId(configId);
    if (tuningWindowStart == 0L && tuningWindowEnd == 0L) {
      // default tuning window 28 days
      tuningWindowEnd = System.currentTimeMillis();
      tuningWindowStart = tuningWindowEnd - TimeUnit.DAYS.toMillis(28);
    }
    info.setTuningWindowStart(tuningWindowStart);
    info.setTuningWindowEnd(tuningWindowEnd);
    info.setEnd(System.currentTimeMillis());
    info.setStart(info.getEnd() - ONBOARDING_REPLAY_LOOKBACK);

    String taskInfoJson = OBJECT_MAPPER.writeValueAsString(info);
    String jobName = String.format("%s_%d", TaskConstants.TaskType.YAML_DETECTION_ONBOARD, configId);

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.YAML_DETECTION_ONBOARD);
    taskDTO.setJobName(jobName);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = this.taskDAO.save(taskDTO);
    LOG.info("Created yaml detection onboarding task {} with taskId {}", taskDTO, taskId);
  }

  private Response processBadRequestResponse(String type, String operation, String payload, IllegalArgumentException e) {
    Map<String, String> responseMessage = new HashMap<>();
    LOG.warn("Validation error while {} {} with payload {}", operation, type, payload, e);
    responseMessage.put("message", "Validation Error in " + type + "! " + e.getMessage());
    if (e.getCause() != null) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.getCause().printStackTrace(pw);
      responseMessage.put("more-info", "Error = " + sw.toString());
    }
    return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
  }

  private Response processServerErrorResponse(String type, String operation, String payload, Exception e) {
    Map<String, String> responseMessage = new HashMap<>();
    LOG.error("Error {} {} with payload {}", operation, type, payload, e);
    responseMessage.put("message", "Failed to create the " + type + ". Reach out to the ThirdEye team.");
    responseMessage.put("more-info", "Error = " + e.getMessage());
    return Response.serverError().entity(responseMessage).build();
  }

  private Response processBadAuthorizationResponse(String type, String operation, String payload, NotAuthorizedException e) {
    Map<String, String> responseMessage = new HashMap<>();
    LOG.warn("Authorization error while {} {} with payload {}", operation, type, payload, e);
    responseMessage.put("message", "Authorization error! You do not have permissions to " + operation + " this " + type + " config");
    responseMessage.put("more-info", "Configure owners property in " + type + " config");
    return Response.status(Response.Status.UNAUTHORIZED).entity(responseMessage).build();
  }

  @POST
  @Path("/create-alert")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @PermitAll
  @ApiOperation("Use yaml to create both subscription and detection yaml. ")
  public Response createYamlAlert(
      @Auth ThirdEyePrincipal user,
      @ApiParam(value =  "a json contains both subscription and detection yaml as string")  String payload,
      @ApiParam("tuning window start time for tunable components") @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) throws Exception {
    Map<String, String> yamls;

    // Detection
    long detectionConfigId;
    try {
      yamls = OBJECT_MAPPER.readValue(payload, Map.class);

      Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");
      Preconditions.checkArgument(yamls.containsKey(PROP_DETECTION), "Detection pipeline yaml is missing");

      detectionConfigId = createDetectionPipeline(yamls.get(PROP_DETECTION), startTime, endTime);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_DETECTION, YamlOperations.CREATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_DETECTION, YamlOperations.CREATING.name(), payload, e);
    }

    // Notification
    long detectionAlertConfigId;
    try {
      Preconditions.checkArgument(yamls.containsKey(PROP_SUBSCRIPTION), "Subscription group yaml is missing.");

      String subscriptionYaml = yamls.get(PROP_SUBSCRIPTION);
      Map<String, Object> subscriptionYamlConfig;
      try {
        subscriptionYamlConfig = ConfigUtils.getMap(this.yaml.load(subscriptionYaml));
      } catch (Exception e) {
        throw new IllegalArgumentException(e.getMessage());
      }

      // Check if existing or new subscription group
      String groupName = MapUtils.getString(subscriptionYamlConfig, PROP_SUBS_GROUP_NAME);
      List<DetectionAlertConfigDTO> alertConfigDTOS = detectionAlertConfigDAO.findByPredicate(Predicate.EQ("name", groupName));
      if (!alertConfigDTOS.isEmpty()) {
        detectionAlertConfigId = alertConfigDTOS.get(0).getId();
        updateSubscriptionGroup(user, detectionAlertConfigId, subscriptionYaml);
      } else {
        detectionAlertConfigId = createSubscriptionGroup(subscriptionYaml);
      }
    } catch (IllegalArgumentException e) {
      this.detectionConfigDAO.deleteById(detectionConfigId);
      return processBadRequestResponse(PROP_SUBSCRIPTION, YamlOperations.CREATING.name() + "/" + YamlOperations.UPDATING.name(), payload, e);
    } catch (NotAuthorizedException e) {
      return processBadAuthorizationResponse(PROP_SUBSCRIPTION, YamlOperations.UPDATING.name(), payload, e);
    } catch (Exception e) {
      this.detectionConfigDAO.deleteById(detectionConfigId);
      return processServerErrorResponse(PROP_DETECTION, YamlOperations.CREATING.name() + "/" + YamlOperations.UPDATING.name(), payload, e);
    }

    // create an yaml onboarding task to run replay and tuning
    createYamlOnboardingTask(detectionConfigId, startTime, endTime);

    LOG.info("Alert created successfully with detection ID " + detectionConfigId + " and subscription ID "
        + detectionAlertConfigId);
    return Response.ok().entity(ImmutableMap.of(
        "detectionConfigId", detectionConfigId,
        "detectionAlertConfigId", detectionAlertConfigId)
    ).build();
  }

  long createDetectionPipeline(String yamlDetectionConfig) {
    return createDetectionPipeline(yamlDetectionConfig, 0, 0);
  }

  long createDetectionPipeline(String payload, long startTime, long endTime)
      throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");

    DetectionConfigDTO detectionConfig = buildDetectionConfigFromYaml(startTime, endTime, payload, null);

    // Check for duplicates
    List<DetectionConfigDTO> detectionConfigDTOS = detectionConfigDAO
        .findByPredicate(Predicate.EQ("name", detectionConfig.getName()));
    Preconditions.checkArgument(detectionConfigDTOS.isEmpty(),
        "Detection name is already taken. Please use a different detectionName.");

    // Validate the detection config before saving it
    detectionValidator.validateConfig(detectionConfig);
    // Save the detection config
    Long id = this.detectionConfigDAO.save(detectionConfig);
    Preconditions.checkNotNull(id, "Error while saving the detection pipeline");

    return detectionConfig.getId();
  }

  /**
   Set up a detection pipeline using a YAML config
   @param payload YAML config string
   @param startTime tuning window start time for tunable components
   @param endTime tuning window end time for tunable components
   @return a message contains the saved detection config id
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Set up a detection pipeline using a YAML config")
  public Response createDetectionPipelineApi(
      @ApiParam("yaml config") String payload,
      @ApiParam("tuning window start time for tunable components") @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) {
    Map<String, String> responseMessage = new HashMap<>();
    long detectionConfigId;
    try {
      detectionConfigId = createDetectionPipeline(payload, startTime, endTime);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_DETECTION, YamlOperations.CREATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_DETECTION, YamlOperations.CREATING.name(), payload, e);
    }

    LOG.info("Detection created with id " + detectionConfigId + " using payload " + payload);
    responseMessage.put("message", "Alert was created successfully.");
    responseMessage.put("more-info", "Record saved with id " + detectionConfigId);
    return Response.ok().entity(responseMessage).build();
  }

  private void updateDetectionPipeline(ThirdEyePrincipal user, long detectionID, String yamlDetectionConfig) {
    updateDetectionPipeline(user, detectionID, yamlDetectionConfig, 0, 0);
  }

  private void updateDetectionPipeline(ThirdEyePrincipal user, long detectionID, String payload, long startTime, long endTime)
      throws IllegalArgumentException {
    DetectionConfigDTO existingDetectionConfig = this.detectionConfigDAO.findById(detectionID);
    DetectionConfigDTO detectionConfig;
    Preconditions.checkNotNull(existingDetectionConfig, "Cannot find detection pipeline " + detectionID);
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");

    authorizeUser(user, detectionID, PROP_DETECTION);
    try {
      detectionConfig = buildDetectionConfigFromYaml(startTime, endTime, payload, existingDetectionConfig);

      // Validate updated config before saving it
      detectionValidator.validateUpdatedConfig(detectionConfig, existingDetectionConfig);
      // Save the detection config
      Long id = this.detectionConfigDAO.save(detectionConfig);
      Preconditions.checkNotNull(id, "Error while saving the detection pipeline");
    } finally {
      // If it is to disable the pipeline then no need to do validation and parsing.
      // It is possible that the metric or dataset was deleted so the validation will fail.
      Map<String, Object> detectionConfigMap = new HashMap<>(ConfigUtils.getMap(this.yaml.load(payload)));
      if (!MapUtils.getBooleanValue(detectionConfigMap, PROP_ACTIVE, true)) {
        existingDetectionConfig.setActive(false);
        existingDetectionConfig.setYaml(payload);
        this.detectionConfigDAO.save(existingDetectionConfig);
      }
    }
  }

  private boolean isServiceAccount(ThirdEyePrincipal user) {
    if (user == null || user.getSessionKey() == null) {
      return false;
    }

    List<Predicate> predicates = new ArrayList<>();
    predicates.add(Predicate.EQ(PROP_SESSION_KEY, user.getSessionKey()));
    predicates.add(Predicate.EQ(PROP_PRINCIPAL_TYPE, PROP_SERVICE));

    List<SessionDTO> sessionDTO = this.sessionDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[0])));
    return sessionDTO != null && !sessionDTO.isEmpty();
  }

  private void validateConfigOwner(ThirdEyePrincipal user, List<String> owners) {
    Preconditions.checkNotNull(user.getName(), "Unable to retrieve the user name from the request");
    if (owners == null || !owners.contains(user.getName())) {
      throw new NotAuthorizedException("Service account " + user.getName() + " is not authorized to access this resource.");
    }
  }

  /**
   * Enforce authorization only with service accounts to prevent risk
   * of modifying other configs when making programmatic calls.
   */
  private void authorizeUser(ThirdEyePrincipal user, long id, String authEntity) {
    if (isServiceAccount(user)) {
      if (authEntity.equals(PROP_DETECTION)) {
        DetectionConfigDTO detectionConfig = this.detectionConfigDAO.findById(id);
        validateConfigOwner(user, detectionConfig.getOwners());
      } else if (authEntity.equals(PROP_SUBSCRIPTION)) {
        DetectionAlertConfigDTO subscriptionConfig = this.detectionAlertConfigDAO.findById(id);
        validateConfigOwner(user, subscriptionConfig.getOwners());
      }

      LOG.info("Service account " + user.getName() + " authorized successfully");
    }
  }

  /**
   Edit a detection pipeline using a YAML config
   @param payload YAML config string
   @param id the detection config id to edit
   @param startTime tuning window start time for tunable components
   @param endTime tuning window end time for tunable components
   @return a message contains the saved detection config id
   */
  @PUT
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @PermitAll
  @ApiOperation("Edit a detection pipeline using a YAML config")
  public Response updateDetectionPipelineApi(
      @Auth ThirdEyePrincipal user,
      @ApiParam("yaml config") String payload,
      @ApiParam("the detection config id to edit") @PathParam("id") long id,
      @ApiParam("tuning window start time for tunable components")  @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      updateDetectionPipeline(user, id, payload, startTime, endTime);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_DETECTION, YamlOperations.UPDATING.name(), payload, e);
    } catch (NotAuthorizedException e) {
      return processBadAuthorizationResponse(PROP_DETECTION, YamlOperations.UPDATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_DETECTION, YamlOperations.UPDATING.name(), payload, e);
    }

    LOG.info("Detection with id " + id + " updated");
    responseMessage.put("message", "Alert was updated successfully.");
    responseMessage.put("detectionConfigId", String.valueOf(id));
    return Response.ok().entity(responseMessage).build();
  }

  private DetectionConfigDTO fetchExistingDetection(String payload) {
    DetectionConfigDTO existingDetectionConfig = null;

    // Extract the detectionName from payload
    Map<String, Object> detectionConfigMap = new HashMap<>();
    detectionConfigMap.putAll(ConfigUtils.getMap(this.yaml.load(payload)));
    String detectionName = MapUtils.getString(detectionConfigMap, PROP_DETECTION_NAME);
    Preconditions.checkNotNull(detectionName, "Missing property detectionName in the detection config.");

    // Check if detection already existing
    Collection<DetectionConfigDTO> detectionConfigs = this.detectionConfigDAO
        .findByPredicate(Predicate.EQ("name", detectionName));
    if (detectionConfigs != null && !detectionConfigs.isEmpty()) {
      existingDetectionConfig = detectionConfigs.iterator().next();
    }

    return existingDetectionConfig;
  }

  long createOrUpdateDetectionPipeline(ThirdEyePrincipal user, String payload) {
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");
    long detectionId;
    DetectionConfigDTO existingDetection = fetchExistingDetection(payload);
    if (existingDetection != null) {
      detectionId = existingDetection.getId();
      updateDetectionPipeline(user, detectionId, payload);
    } else {
      detectionId = createDetectionPipeline(payload);
    }

    return detectionId;
  }

  /**
   Set up a detection pipeline using a YAML config - create new or update existing
   @param payload YAML config string
   @return a message contains the saved detection config id
   */
  @POST
  @Path("/create-or-update")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @PermitAll
  @ApiOperation("Create a new detection pipeline or update existing if one already exists")
  public Response createOrUpdateDetectionPipelineApi(
      @Auth ThirdEyePrincipal user,
      @ApiParam("yaml config") String payload) {
    Map<String, String> responseMessage = new HashMap<>();
    long detectionConfigId;
    try {
      detectionConfigId = createOrUpdateDetectionPipeline(user, payload);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_DETECTION, YamlOperations.CREATING.name() + "/" + YamlOperations.UPDATING.name(), payload, e);
    } catch (NotAuthorizedException e) {
      return processBadAuthorizationResponse(PROP_DETECTION, YamlOperations.CREATING.name() + "/" + YamlOperations.UPDATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_DETECTION, YamlOperations.CREATING.name() + "/" + YamlOperations.UPDATING.name(), payload, e);
    }

    LOG.info("Detection Pipeline created/updated with id " + detectionConfigId + " using payload " + payload);
    responseMessage.put("message", "The alert was created/updated successfully.");
    responseMessage.put("more-info", "Record saved/updated with id " + detectionConfigId);
    return Response.ok().entity(responseMessage).build();
  }

  public long createSubscriptionGroup(String yamlConfig) throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(yamlConfig),
        "The Yaml Payload in the request is empty.");

    DetectionAlertConfigDTO alertConfig = new SubscriptionConfigTranslator(detectionConfigDAO, yamlConfig).translate();

    // Check for duplicates
    List<DetectionAlertConfigDTO> alertConfigDTOS = detectionAlertConfigDAO
        .findByPredicate(Predicate.EQ("name", alertConfig.getName()));
    Preconditions.checkArgument(alertConfigDTOS.isEmpty(),
        "Subscription group name is already taken. Please use a different name.");

    // Validate the config before saving it
    subscriptionValidator.validateConfig(alertConfig);

    // Save the detection alert config
    Long id = this.detectionAlertConfigDAO.save(alertConfig);
    Preconditions.checkNotNull(id, "Error while saving the subscription group");

    return alertConfig.getId();
  }

  @POST
  @Path("/subscription")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Create a subscription group using a YAML config")
  @SuppressWarnings("unchecked")
  public Response createSubscriptionGroupApi(
      @ApiParam("payload") String payload) {
    Map<String, String> responseMessage = new HashMap<>();
    long detectionAlertConfigId;
    try {
      detectionAlertConfigId = createSubscriptionGroup(payload);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_SUBSCRIPTION, YamlOperations.CREATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_SUBSCRIPTION, YamlOperations.CREATING.name(), payload, e);
    }

    LOG.info("Notification group created with id " + detectionAlertConfigId + " using payload " + payload);
    responseMessage.put("message", "The subscription group was created successfully.");
    responseMessage.put("detectionAlertConfigId", String.valueOf(detectionAlertConfigId));
    return Response.ok().entity(responseMessage).build();
  }

  /**
   * Update the existing {@code oldAlertConfig} with the new {@code newAlertConfig}
   *
   * Update all the fields except the vector clocks and high watermark. The clocks and watermarks
   * are managed by the platform. They shouldn't be reset by the user.
   */
  private DetectionAlertConfigDTO updateDetectionAlertConfig(DetectionAlertConfigDTO oldAlertConfig,
      DetectionAlertConfigDTO newAlertConfig) {
    oldAlertConfig.setName(newAlertConfig.getName());
    oldAlertConfig.setCronExpression(newAlertConfig.getCronExpression());
    oldAlertConfig.setApplication(newAlertConfig.getApplication());
    oldAlertConfig.setFrom(newAlertConfig.getFrom());
    oldAlertConfig.setSubjectType(newAlertConfig.getSubjectType());
    oldAlertConfig.setReferenceLinks(newAlertConfig.getReferenceLinks());
    oldAlertConfig.setActive(newAlertConfig.isActive());
    oldAlertConfig.setAlertSchemes(newAlertConfig.getAlertSchemes());
    oldAlertConfig.setAlertSuppressors(newAlertConfig.getAlertSuppressors());
    oldAlertConfig.setProperties(newAlertConfig.getProperties());
    oldAlertConfig.setYaml(newAlertConfig.getYaml());
    oldAlertConfig.setOwners(newAlertConfig.getOwners());

    return oldAlertConfig;
  }

  void updateSubscriptionGroup(ThirdEyePrincipal user, long oldAlertConfigID, String yamlConfig) {
    DetectionAlertConfigDTO oldAlertConfig = this.detectionAlertConfigDAO.findById(oldAlertConfigID);
    if (oldAlertConfig == null) {
      throw new RuntimeException("Cannot find subscription group " + oldAlertConfigID);
    }
    Preconditions.checkArgument(StringUtils.isNotBlank(yamlConfig), "The Yaml Payload in the request is empty.");

    authorizeUser(user, oldAlertConfigID, PROP_SUBSCRIPTION);
    DetectionAlertConfigDTO newAlertConfig = new SubscriptionConfigTranslator(detectionConfigDAO, yamlConfig).translate();
    DetectionAlertConfigDTO updatedAlertConfig = updateDetectionAlertConfig(oldAlertConfig, newAlertConfig);

    // Update watermarks to reflect changes to detectionName list in subscription config
    Map<Long, Long> currentVectorClocks = updatedAlertConfig.getVectorClocks();
    Map<Long, Long> updatedVectorClocks = new HashMap<>();
    Map<String, Object> properties = updatedAlertConfig.getProperties();
    long currentTimestamp = System.currentTimeMillis();
    if (properties.get(PROP_DETECTION_CONFIG_IDS) != null) {
      for (long detectionId : ConfigUtils.getLongs(properties.get(PROP_DETECTION_CONFIG_IDS))) {
        if (currentVectorClocks != null && currentVectorClocks.keySet().contains(detectionId)) {
          updatedVectorClocks.put(detectionId, currentVectorClocks.get(detectionId));
        } else {
          updatedVectorClocks.put(detectionId, currentTimestamp);
        }
      }
    }

    updatedAlertConfig.setVectorClocks(updatedVectorClocks);

    // Validate before updating the config
    subscriptionValidator.validateUpdatedConfig(updatedAlertConfig, oldAlertConfig);

    // Save the updated subscription config
    int detectionAlertConfigId = this.detectionAlertConfigDAO.update(updatedAlertConfig);
    if (detectionAlertConfigId <= 0) {
      throw new RuntimeException("Failed to update the detection alert config.");
    }
  }

  @PUT
  @Path("/subscription/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @PermitAll
  @ApiOperation("Edit a subscription group using a YAML config")
  public Response updateSubscriptionGroupApi(
      @Auth ThirdEyePrincipal user,
      @ApiParam("payload") String payload,
      @ApiParam("the detection alert config id to edit") @PathParam("id") long id) {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      updateSubscriptionGroup(user, id, payload);
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(PROP_SUBSCRIPTION, YamlOperations.UPDATING.name(), payload, e);
    } catch (NotAuthorizedException e) {
      return processBadAuthorizationResponse(PROP_SUBSCRIPTION, YamlOperations.UPDATING.name(), payload, e);
    } catch (Exception e) {
      return processServerErrorResponse(PROP_SUBSCRIPTION, YamlOperations.UPDATING.name(), payload, e);
    }

    LOG.info("Subscription group with id " + id + " updated");
    responseMessage.put("message", "The subscription group was updated successfully.");
    responseMessage.put("detectionAlertConfigId", String.valueOf(id));
    return Response.ok().entity(responseMessage).build();
  }

  @POST
  @Path("/preview")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Preview the anomaly detection result of a YAML configuration")
  public Response yamlPreviewApi(
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("tuningStart") long tuningStart,
      @QueryParam("tuningEnd") long tuningEnd,
      @ApiParam("jsonPayload") String payload) {
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");
    return runPreview(start, end, tuningStart, tuningEnd, payload, null);
  }

  @POST
  @Path("/preview/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Preview the anomaly detection result of a YAML configuration, with an existing config and historical anomalies")
  public Response yamlPreviewWithHistoricalAnomalies(
      @PathParam("id") long id,
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("tuningStart") long tuningStart,
      @QueryParam("tuningEnd") long tuningEnd,
      @ApiParam("jsonPayload") String payload) {
    Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");
    DetectionConfigDTO existingConfig = this.detectionConfigDAO.findById(id);
    Preconditions.checkNotNull(existingConfig, "can not find existing detection config " + id);
    return runPreview(start, end, tuningStart, tuningEnd, payload, existingConfig);
  }

  private Response runPreview(long start, long end,
      long tuningStart, long tuningEnd, String payload, DetectionConfigDTO existingConfig) {
    long ts = System.currentTimeMillis();
    Map<String, String> responseMessage = new HashMap<>();
    DetectionPipelineResult result;
    Future<DetectionPipelineResult> future = null;
    try {
      DetectionConfigDTO detectionConfig;
      if (existingConfig == null) {
        detectionConfig = buildDetectionConfigFromYaml(tuningStart, tuningEnd, payload, null);
        detectionConfig.setId(Long.MAX_VALUE);
      } else {
        detectionConfig = buildDetectionConfigFromYaml(tuningStart, tuningEnd, payload, existingConfig);
      }

      Preconditions.checkNotNull(detectionConfig);
      DetectionPipeline pipeline = this.loader.from(this.provider, detectionConfig, start, end);
      future = this.executor.submit(pipeline::run);
      result = future.get(this.previewTimeout, TimeUnit.MILLISECONDS);
      LOG.info("Preview successful, used {} milliseconds", System.currentTimeMillis() - ts);
      return Response.ok(result).build();
    } catch (IllegalArgumentException e) {
      return processBadRequestResponse(YamlOperations.PREVIEW.name(), YamlOperations.RUNNING.name(), payload, e);
    } catch (TimeoutException e) {
      responseMessage.put("message", "Preview has timed out");
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error running preview with payload " + payload, e);
      StringBuilder sb = new StringBuilder();
      // show more stack message to frontend for debugging
      getErrorMessage(0, 5, e, sb);
      responseMessage.put("message", "Failed to run the preview. Error stack: " + sb.toString());
      return Response.serverError().entity(responseMessage).build();
    } finally {
      // stop the preview
      if (future != null) {
        future.cancel(true);
      }
    }
  }

  private void getErrorMessage(int curLevel, int totalLevel, Throwable e, StringBuilder sb) {
    if (curLevel <= totalLevel && e != null) {
      sb.append("==");
      if (e.getMessage() != null) {
        sb.append(e.getMessage());
      }
      getErrorMessage(curLevel + 1, totalLevel, e.getCause(), sb);
    }
  }

  @POST
  @Path("/preview/baseline")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Get baseline from YAML configuration")
  /* TODO: Will return baseline from yamlPreviewApi together with detection in the future. */
  public Response yamlPreviewBaselineApi(
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("urn") @NotNull String urn,
      @QueryParam("tuningStart") long tuningStart,
      @QueryParam("tuningEnd") long tuningEnd,
      @ApiParam("jsonPayload") @NotNull String payload,
      @QueryParam("ruleName") String ruleName) {
    try {
      Preconditions.checkArgument(StringUtils.isNotBlank(payload), "The Yaml Payload in the request is empty.");

      DetectionConfigDTO detectionConfig = buildDetectionConfigFromYaml(tuningStart, tuningEnd, payload, null);
      Preconditions.checkNotNull(detectionConfig);
      detectionConfig.setId(Long.MAX_VALUE);

      // There is a side effect to update detectionConfig when loading the pipeline.
      this.loader.from(this.provider, detectionConfig, start, end);
      TimeSeries baseline = getBaseline(detectionConfig, start, end, urn, ruleName);

      return Response.ok(makeTimeSeriesMap(baseline)).build();
    } catch (Exception e) {
      LOG.error("Error getting baseline with payload " + payload, e);
    }
    return Response.ok().build();
  }

  /**
   * Returns a map of time/baseline/current/upper/lower time series derived from the TimeSeries.
   *
   * @param baseline Baseline values.
   * @return map of time/baseline/current/upper/lower time series.
   */
  private static Map<String, List<? extends Number>> makeTimeSeriesMap(TimeSeries baseline) {
    Map<String, List<? extends Number>> output = new HashMap<>();
    // time and baseline are mandatory
    output.put(COL_TIME, baseline.getTime().toList());
    output.put(COL_VALUE, baseline.getPredictedBaseline().toList());
    if (baseline.getDataFrame().contains(COL_CURRENT)) {
      output.put(COL_CURRENT, baseline.getCurrent().toList());
    }
    if (baseline.getDataFrame().contains(COL_UPPER_BOUND)) {
      output.put(COL_UPPER_BOUND, baseline.getPredictedUpperBound().toList());
    }
    if (baseline.getDataFrame().contains(COL_LOWER_BOUND)) {
      output.put(COL_LOWER_BOUND, baseline.getPredictedLowerBound().toList());
    }
    return output;
  }

  /**
   * Get the baselines for metric urn.
   * If there are multiple rules return the first rule's baseline.
   * TODO: The baseline should be calculated together with detection in the future.
   *
   * @param detectionConfig The detection configuration.
   * @param start Start time for baseline calculation.
   * @param end End time for baseline calculation.
   * @param urn The metric urn.
   * @param rule The rule name. If not provided then find the first rule.
   * @return The baseline for the urn.
   */
  private TimeSeries getBaseline(DetectionConfigDTO detectionConfig, long start, long end, String urn, String rule) {
    MetricEntity metric = MetricEntity.fromURN(urn);
    MetricSlice slice = MetricSlice.from(metric.getId(), start, end, metric.getFilters(), MetricSlice.NATIVE_GRANULARITY);

    Optional<BaselineProvider> provider =  detectionConfig.getComponents().entrySet().stream()
        .filter(x -> x.getValue() instanceof BaselineProvider && (rule == null || rule.isEmpty() || x.getKey().startsWith(rule)))
        .map(x -> (BaselineProvider) x.getValue())
        .findFirst();

    if (provider.isPresent()) {
      return provider.get().computePredictedTimeSeries(slice);
    }

    return TimeSeries.empty();
  }

  /**
   * Toggle active/inactive for given detection
   *
   * @param detectionId detection config id (must exist)
   * @param active value to set for active field in detection config
   */
  @PUT
  @Path("/activation/{id}")
  @ApiOperation("Make detection active or inactive, given id")
  @Produces(MediaType.APPLICATION_JSON)
  public Response toggleActivation(
      @ApiParam("Detection configuration id for the alert") @NotNull @PathParam("id") long detectionId,
      @ApiParam("Active status you want to set for the alert") @NotNull @QueryParam("active") boolean active) throws Exception {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      DetectionConfigDTO config = this.detectionConfigDAO.findById(detectionId);
      if (config == null) {
        throw new IllegalArgumentException(String.format("Cannot find config %d", detectionId));
      }

      // update state
      config.setActive(active);
      this.detectionConfigDAO.update(config);
      responseMessage.put("message", "Alert activation toggled to " + active + " for detection id " + detectionId);
    } catch (Exception e) {
      LOG.error("Error toggling activation on detection id " + detectionId, e);
      responseMessage.put("message", "Failed to toggle activation: " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Alert activation toggled to {} for detection id {}", active , detectionId);
    return Response.ok(responseMessage).build();
  }


  /**
   * List all yaml configurations as JSON enhanced with detection config id, isActive and createBy information.
   * @return the yaml configuration converted in to JSON, with enhanced information from detection config DTO.
   */
  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Map<String, Object>> listYamls() throws ExecutionException {
    return this.detectionConfigDAO.findAll().parallelStream().map(this.detectionConfigFormatter::format).collect(Collectors.toList());
  }
}
