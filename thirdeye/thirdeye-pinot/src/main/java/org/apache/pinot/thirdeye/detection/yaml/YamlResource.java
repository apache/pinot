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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.ValidationException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
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
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.validators.SubscriptionConfigValidator;
import org.apache.pinot.thirdeye.detection.onboard.YamlOnboardingTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


@Path("/yaml")
@Api(tags = {Constants.YAML_TAG})
public class YamlResource {
  protected static final Logger LOG = LoggerFactory.getLogger(YamlResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  private static final long ONBOARDING_REPLAY_LOOKBACK = TimeUnit.DAYS.toMillis(30);

  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final YamlDetectionTranslatorLoader translatorLoader;
  private final YamlDetectionAlertConfigTranslator alertConfigTranslator;
  private final DetectionConfigValidator detectionValidator;
  private final SubscriptionConfigValidator subscriptionValidator;
  private final DataProvider provider;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final TaskManager taskDAO;
  private final DetectionPipelineLoader loader;
  private final Yaml yaml;

  public YamlResource() {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.translatorLoader = new YamlDetectionTranslatorLoader();
    this.detectionValidator = DetectionConfigValidator.getInstance();
    this.subscriptionValidator = SubscriptionConfigValidator.getInstance();
    this.alertConfigTranslator = new YamlDetectionAlertConfigTranslator(this.detectionConfigDAO);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.yaml = new Yaml();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, timeseriesLoader, aggregationLoader, loader);
  }

  public DetectionConfigDTO translateToDetectionConfig(Map<String, Object> yamlConfig) throws Exception {
    return buildDetectionConfigFromYaml(0, 0, yamlConfig, null);
  }

  /*
   * Build the detection config from a yaml.
   */
  private DetectionConfigDTO buildDetectionConfigFromYaml(long tuningStartTime, long tuningEndTime, Map<String, Object> yamlConfig,
      DetectionConfigDTO existingDetectionConfig) throws Exception {

    // Configure the tuning window
    if (tuningStartTime == 0L && tuningEndTime == 0L) {
      // default tuning window 28 days
      tuningEndTime = System.currentTimeMillis();
      tuningStartTime = tuningEndTime - TimeUnit.DAYS.toMillis(28);
    }

    YamlDetectionConfigTranslator translator = this.translatorLoader.from(yamlConfig, this.provider);
    return translator.withTuningWindow(tuningStartTime, tuningEndTime)
        .withExistingDetectionConfig(existingDetectionConfig)
        .generateDetectionConfig();
  }

  /*
   * Create a yaml onboarding task. It runs 1 month replay and re-tune the pipeline.
   */
  private void createYamlOnboardingTask(long configId, long tuningWindowStart, long tuningWindowEnd){
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

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(info);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting yaml detection onboarding {} to jsonString", info, e);
    }
    String jobName = String.format("%s_%d", TaskConstants.TaskType.YAML_DETECTION_ONBOARD, configId);

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.YAML_DETECTION_ONBOARD);
    taskDTO.setJobName(jobName);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = this.taskDAO.save(taskDTO);
    LOG.info("Created yaml detection onboarding task {} with taskId {}", taskDTO, taskId);
  }


  @POST
  @Path("/create-alert")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Use yaml to create both subscription and detection yaml. ")
  public Response createYamlAlert(@ApiParam(value =  "a json contains both subscription and detection yaml as string")  String payload,
      @ApiParam("tuning window start time for tunable components") @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) throws Exception{
    Map<String, String> yamls = OBJECT_MAPPER.readValue(payload, Map.class);
    if (StringUtils.isBlank(payload)){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "Empty payload")).build();
    }
    if (!yamls.containsKey("detection")) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection pipeline yaml is missing")).build();
    }
    if (!yamls.containsKey("notification")){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "subscription group yaml is missing")).build();
    }

    // Detection
    String detectionYaml = yamls.get("detection");
    long detectionConfigId = createDetectionPipeline(detectionYaml, startTime, endTime);

    // Notification
    String notificationYaml = yamls.get("notification");

    Map<String, Object> notificationYamlConfig;
    try {
      notificationYamlConfig = ConfigUtils.getMap(this.yaml.load(notificationYaml));
    } catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(ImmutableMap.of("message", "notification yaml parsing error, " + e.getMessage())).build();
    }

    // Check if existing or new subscription group
    String groupName = MapUtils.getString(notificationYamlConfig, PROP_SUBS_GROUP_NAME);
    List<DetectionAlertConfigDTO> alertConfigDTOS = detectionAlertConfigDAO
        .findByPredicate(Predicate.EQ("name", groupName));
    Response response;
    if (!alertConfigDTOS.isEmpty()) {
      response = updateSubscriptionGroupApi(notificationYaml, alertConfigDTOS.get(0).getId());
    } else {
      response = createSubscriptionGroupApi(notificationYaml);
    }

    if (response.getStatusInfo() != Response.Status.OK) {
      // revert detection DTO
      this.detectionConfigDAO.deleteById(detectionConfigId);
      return Response.serverError().entity(response.getEntity()).build();
    }
    long alertId = Long.parseLong(ConfigUtils.getMap(response.getEntity()).get("detectionAlertConfigId").toString());
    // create an yaml onboarding task to run replay and tuning
    createYamlOnboardingTask(detectionConfigId, startTime, endTime);
    return Response.ok().entity(ImmutableMap.of("detectionConfigId", detectionConfigId, "detectionAlertConfigId", alertId)).build();
  }

  long createDetectionPipeline(String yamlDetectionConfig, long startTime, long endTime) throws Exception {
    detectionValidator.validateYAMLConfig(yamlDetectionConfig);

    // Translate config from YAML to detection config (JSON)
    TreeMap<String, Object> newDetectionConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newDetectionConfigMap.putAll(ConfigUtils.getMap(this.yaml.load(yamlDetectionConfig)));
    DetectionConfigDTO detectionConfig = buildDetectionConfigFromYaml(startTime, endTime, newDetectionConfigMap, null);
    detectionConfig.setYaml(yamlDetectionConfig);

    // Check for duplicates
    List<DetectionConfigDTO> detectionConfigDTOS = detectionConfigDAO
        .findByPredicate(Predicate.EQ("name", detectionConfig.getName()));
    if (!detectionConfigDTOS.isEmpty()) {
      throw new ValidationException("Detection name is already taken. Please use a different detectionName.");
    }

    // Validate the config before saving it
    detectionValidator.validateConfig(detectionConfig);

    // Save the detection config
    Preconditions.checkNotNull(detectionConfig);
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
    } catch (ValidationException e) {
      LOG.warn("Validation error while creating detection pipeline with payload " + payload, e);
      responseMessage.put("message", "Validation Error! " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error creating subscription group with payload " + payload, e);
      responseMessage.put("message", "Failed to create the subscription group. Reach out to the ThirdEye team.");
      responseMessage.put("more-info", "Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Detection Pipeline created with id " + detectionConfigId + " using payload " + payload);
    responseMessage.put("message", "The subscription group was created successfully.");
    responseMessage.put("more-info", "Record saved with id " + detectionConfigId);
    return Response.ok().entity(responseMessage).build();
  }

  void updateDetectionPipeline(long detectionID, String yamlDetectionConfig, long startTime, long endTime) throws Exception {
    DetectionConfigDTO existingDetectionConfig = this.detectionConfigDAO.findById(detectionID);
    if (existingDetectionConfig == null) {
      throw new RuntimeException("Cannot find detection pipeline " + detectionID);
    }
    detectionValidator.validateYAMLConfig(yamlDetectionConfig);

    // Translate config from YAML to detection config (JSON)
    TreeMap<String, Object> newDetectionConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newDetectionConfigMap.putAll(ConfigUtils.getMap(this.yaml.load(yamlDetectionConfig)));
    DetectionConfigDTO detectionConfig = buildDetectionConfigFromYaml(startTime, endTime, newDetectionConfigMap, existingDetectionConfig);
    detectionConfig.setYaml(yamlDetectionConfig);

    // Validate updated config before saving it
    detectionValidator.validateUpdatedConfig(detectionConfig, existingDetectionConfig);

    // Save the detection config
    Preconditions.checkNotNull(detectionConfig);
    Long id = this.detectionConfigDAO.save(detectionConfig);
    Preconditions.checkNotNull(id, "Error while saving the detection pipeline");
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
  @ApiOperation("Edit a detection pipeline using a YAML config")
  public Response updateDetectionPipelineApi(
      @ApiParam("yaml config") String payload,
      @ApiParam("the detection config id to edit") @PathParam("id") long id,
      @ApiParam("tuning window start time for tunable components")  @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      updateDetectionPipeline(id, payload, startTime, endTime);
    } catch (ValidationException e) {
      LOG.warn("Validation error while creating detection pipeline with payload " + payload, e);
      responseMessage.put("message", "Validation Error! " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error creating subscription group with payload " + payload, e);
      responseMessage.put("message", "Failed to create the subscription group. Reach out to the ThirdEye team.");
      responseMessage.put("more-info", "Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Detection Pipeline " + id + " updated successfully with payload " + payload);
    responseMessage.put("message", "The detection Pipeline was created successfully.");
    responseMessage.put("detectionConfigId", String.valueOf(id));
    return Response.ok().entity(responseMessage).build();
  }

  long createSubscriptionGroup(String yamlAlertConfig) throws ValidationException {
    subscriptionValidator.validateYAMLConfig(yamlAlertConfig);

    // Translate config from YAML to detection alert config (JSON)
    TreeMap<String, Object> newAlertConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newAlertConfigMap.putAll(ConfigUtils.getMap(this.yaml.load(yamlAlertConfig)));
    DetectionAlertConfigDTO alertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);
    alertConfig.setYaml(yamlAlertConfig);

    // Check for duplicates
    List<DetectionAlertConfigDTO> alertConfigDTOS = detectionAlertConfigDAO
        .findByPredicate(Predicate.EQ("name", alertConfig.getName()));
    if (!alertConfigDTOS.isEmpty()) {
      throw new ValidationException("Subscription group name is already taken. Please use a different name.");
    }

    // Validate the config before saving it
    subscriptionValidator.validateConfig(alertConfig);

    // Save the detection alert config
    Preconditions.checkNotNull(alertConfig);
    Long id = this.detectionAlertConfigDAO.save(alertConfig);
    Preconditions.checkNotNull(id, "Error while saving the subscription group");

    return alertConfig.getId();
  }

  @POST
  @Path("/notification")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Create a notification group using a YAML config")
  @SuppressWarnings("unchecked")
  public Response createSubscriptionGroupApi(
      @ApiParam("payload") String yamlAlertConfig) {
    Map<String, String> responseMessage = new HashMap<>();
    long detectionAlertConfigId;
    try {
      detectionAlertConfigId = createSubscriptionGroup(yamlAlertConfig);
    } catch (ValidationException e) {
      LOG.warn("Validation error while creating subscription group with payload " + yamlAlertConfig, e);
      responseMessage.put("message", "Validation Error! " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error creating subscription group with payload " + yamlAlertConfig, e);
      responseMessage.put("message", "Failed to create the subscription group. Reach out to the ThirdEye team.");
      responseMessage.put("more-info", "Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Notification group created with id " + detectionAlertConfigId + " using payload " + yamlAlertConfig);
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
    oldAlertConfig.setOnlyFetchLegacyAnomalies(newAlertConfig.isOnlyFetchLegacyAnomalies());
    oldAlertConfig.setProperties(newAlertConfig.getProperties());

    return oldAlertConfig;
  }

  void updateSubscriptionGroup(long oldAlertConfigID, String yamlAlertConfig) throws ValidationException {
    DetectionAlertConfigDTO oldAlertConfig = this.detectionAlertConfigDAO.findById(oldAlertConfigID);
    if (oldAlertConfig == null) {
      throw new RuntimeException("Cannot find subscription group " + oldAlertConfigID);
    }
    subscriptionValidator.validateYAMLConfig(yamlAlertConfig);

    // Translate payload to detection alert config
    TreeMap<String, Object> newAlertConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newAlertConfigMap.putAll(ConfigUtils.getMap(this.yaml.load(yamlAlertConfig)));
    DetectionAlertConfigDTO newAlertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);

    // Update existing alert config with the newly supplied config.
    DetectionAlertConfigDTO updatedAlertConfig = updateDetectionAlertConfig(oldAlertConfig, newAlertConfig);
    updatedAlertConfig.setYaml(yamlAlertConfig);

    // Validate before updating the config
    subscriptionValidator.validateUpdatedConfig(updatedAlertConfig, oldAlertConfig);

    // Save the updated subscription config
    int detectionAlertConfigId = this.detectionAlertConfigDAO.update(updatedAlertConfig);
    if (detectionAlertConfigId <= 0) {
      throw new RuntimeException("Failed to update the detection alert config.");
    }
  }

  @PUT
  @Path("/notification/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Edit a notification group using a YAML config")
  public Response updateSubscriptionGroupApi(
      @ApiParam("payload") String yamlAlertConfig,
      @ApiParam("the detection alert config id to edit") @PathParam("id") long id) {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      updateSubscriptionGroup(id, yamlAlertConfig);
    } catch (ValidationException e) {
      LOG.warn("Validation error while updating subscription group " + id + " with payload " + yamlAlertConfig, e);
      responseMessage.put("message", "Validation Error! " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error updating subscription group " + id + " with payload " + yamlAlertConfig, e);
      responseMessage.put("message", "Failed to update the subscription group. Reach out to the ThirdEye team.");
      responseMessage.put("more-info", "Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Notification group " + id + " updated successfully with payload " + yamlAlertConfig);
    responseMessage.put("message", "The YAML alert config was updated successfully.");
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
      @ApiParam("jsonPayload") String payload) throws Exception {
    Map<String, String> responseMessage = new HashMap<>();
    DetectionPipelineResult result;
    try {
      detectionValidator.validateYAMLConfig(payload);

      // Translate config from YAML to detection config (JSON)
      Map<String, Object> newDetectionConfigMap = new HashMap<>(ConfigUtils.getMap(this.yaml.load(payload)));
      DetectionConfigDTO detectionConfig = buildDetectionConfigFromYaml(tuningStart, tuningEnd, newDetectionConfigMap, null);
      Preconditions.checkNotNull(detectionConfig);
      detectionConfig.setId(Long.MAX_VALUE);

      DetectionPipeline pipeline = this.loader.from(this.provider, detectionConfig, start, end);
      result = pipeline.run();

    } catch (ValidationException e) {
      LOG.warn("Validation error while running preview with payload  " + payload, e);
      responseMessage.put("message", "Validation Error! " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error running preview with payload " + payload, e);
      responseMessage.put("message", "Failed to run the preview due to " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("Preview successful using payload " + payload);
    return Response.ok(result).build();
  }


  /**
   List all yaml configurations as JSON. enhanced with detection config id, isActive and createBy information.
   @param id id of a specific detection config yaml to list (optional)
   @return the yaml configuration converted in to JSON, with enhanced information from detection config DTO.
   */
  @GET
  @Path("/list")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Object> listYamls(@QueryParam("id") Long id){
    List<DetectionConfigDTO> detectionConfigDTOs;
    if (id == null) {
      detectionConfigDTOs = this.detectionConfigDAO.findAll();
    } else {
      detectionConfigDTOs = Collections.singletonList(this.detectionConfigDAO.findById(id));
    }

    List<Object> yamlObjects = new ArrayList<>();
    for (DetectionConfigDTO detectionConfigDTO : detectionConfigDTOs) {
      if (detectionConfigDTO.getYaml() != null) {
        Map<String, Object> yamlObject = new HashMap<>();
        yamlObject.putAll((Map<? extends String, ?>) this.yaml.load(detectionConfigDTO.getYaml()));
        yamlObject.put("id", detectionConfigDTO.getId());
        yamlObject.put("isActive", detectionConfigDTO.isActive());
        yamlObject.put("createdBy", detectionConfigDTO.getCreatedBy());
        yamlObjects.add(yamlObject);
      }
    }
    return yamlObjects;
  }
}
