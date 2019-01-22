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
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiImplicitParam;
import com.wordnik.swagger.annotations.ApiImplicitParams;
import com.wordnik.swagger.annotations.ApiOperation;
import org.apache.pinot.thirdeye.detection.validators.DetectionAlertConfigValidator;
import com.wordnik.swagger.annotations.ApiParam;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


@Path("/yaml")
@Api(tags = {Constants.YAML_TAG})
public class YamlResource {
  protected static final Logger LOG = LoggerFactory.getLogger(YamlResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  public static final String PROP_DETECTION_NAME = "detectionName";

  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final YamlDetectionTranslatorLoader translatorLoader;
  private final YamlDetectionAlertConfigTranslator alertConfigTranslator;
  private final DetectionAlertConfigValidator alertValidator;
  private final DataProvider provider;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionPipelineLoader loader;
  private final Yaml yaml;

  public YamlResource() {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.translatorLoader = new YamlDetectionTranslatorLoader();
    this.alertValidator = DetectionAlertConfigValidator.getInstance();
    this.alertConfigTranslator = new YamlDetectionAlertConfigTranslator(this.detectionConfigDAO);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.yaml = new Yaml();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, timeseriesLoader, aggregationLoader, loader);
  }

  @POST
  @Path("/create-alert")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Use yaml to create both notification and detection yaml. ")
  public Response createYamlAlert(@ApiParam(value =  "a json contains both notification and detection yaml as string")  String payload,
      @ApiParam("tuning window start time for tunable components") @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) throws Exception{
    Map<String, String> yamls = OBJECT_MAPPER.readValue(payload, Map.class);

    if (StringUtils.isBlank(payload)){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "Empty payload")).build();
    }
    if (!yamls.containsKey("detection")) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection yaml is missing")).build();
    }
    if (!yamls.containsKey("notification")){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "notification yaml is missing")).build();
    }

    // get detection yaml
    String detectionYaml = yamls.get("detection");

    Map<String, Object> detectionYamlConfig;
    try {
      detectionYamlConfig = (Map<String, Object>) this.yaml.load(detectionYaml);
    } catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection yaml parsing error, " + e.getMessage())).build();
    }

    // check if detection config already exists
    String name = MapUtils.getString(detectionYamlConfig, PROP_DETECTION_NAME);
    List<DetectionConfigDTO> detectionConfigDTOs = this.detectionConfigDAO.findByPredicate(
        Predicate.EQ("name", name));
    if (!detectionConfigDTOs.isEmpty()){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection name already exist: " + name )).build();
    }

    HashMap<String, String> responseMessage = new HashMap<>();
    DetectionConfigDTO detectionConfig =
        buildDetectionConfigFromYaml(startTime, endTime, detectionYamlConfig, null, responseMessage);
    if (detectionConfig == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    detectionConfig.setYaml(detectionYaml);
    Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
    if (detectionConfigId == null){
      return Response.serverError().entity(ImmutableMap.of("message", "Save detection config failed")).build();
    }
    Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");

    // notification
    // TODO: Inject detectionConfigId into detection alert config
    DetectionAlertConfigDTO alertConfig = createDetectionAlertConfig(yamls.get("notification"), responseMessage);
    if (alertConfig == null) {
      // revert detection DTO
      this.detectionConfigDAO.deleteById(detectionConfigId);
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    Long detectionAlertConfigId = this.detectionAlertConfigDAO.save(alertConfig);
    if (detectionAlertConfigId == null){
      // revert detection DTO
      this.detectionConfigDAO.deleteById(detectionConfigId);
      return Response.serverError().entity(ImmutableMap.of("message", "Save detection alert config failed")).build();
    }
    LOG.info("saved detection alert config id {}", detectionAlertConfigId);

    return Response.ok().entity(ImmutableMap.of("detectionConfigId", detectionConfig.getId(), "detectionAlertConfigId", alertConfig.getId())).build();
  }

  public DetectionConfigDTO translateToDetectionConfig(Map<String, Object> yamlConfig, Map<String, String> responseMessage) {
    return buildDetectionConfigFromYaml(0, 0, yamlConfig, null, responseMessage);
  }

  /*
   * Build the detection config from a yaml.
   * Returns null if building or validation failed. Error messages stored in responseMessage.
   */
  private DetectionConfigDTO buildDetectionConfigFromYaml(long startTime, long endTime, Map<String, Object> yamlConfig,
      DetectionConfigDTO existingDetectionConfig, Map<String, String> responseMessage) {
    try{
      YamlDetectionConfigTranslator translator = this.translatorLoader.from(yamlConfig, this.provider);
      DetectionConfigDTO detectionConfig = translator.withTrainingWindow(startTime, endTime)
          .withExistingDetectionConfig(existingDetectionConfig)
          .generateDetectionConfig();
      validatePipeline(detectionConfig);
      return detectionConfig;
    } catch (InvocationTargetException e){
      // exception thrown in validate pipeline via reflection
      LOG.error("Validate pipeline error", e);
      responseMessage.put("message", e.getCause().getMessage());
    } catch (Exception e) {
      LOG.error("yaml translation error", e);
      responseMessage.put("message", e.getMessage());
    }
    return null;
  }

  /*
   * Init the pipeline to check if detection pipeline property is valid semantically.
   */
  private void validatePipeline(DetectionConfigDTO detectionConfig) throws Exception {
    Long id = detectionConfig.getId();
    // swap out id
    detectionConfig.setId(-1L);
    // try to load the detection pipeline and init all the components
    this.loader.from(provider, detectionConfig, 0, 0);
    // set id back
    detectionConfig.setId(id);
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
  public Response setUpDetectionPipeline(
      @ApiParam("yaml config") String payload,
      @ApiParam("tuning window start time for tunable components") @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) {
    if (StringUtils.isBlank(payload)){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "empty payload")).build();
    }
    Map<String, Object> yamlConfig;
    try {
      yamlConfig = (Map<String, Object>) this.yaml.load(payload);
    } catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection yaml parsing error, " + e.getMessage())).build();
    }

    // check if detection config already exists
    String name = MapUtils.getString(yamlConfig, PROP_DETECTION_NAME);
    List<DetectionConfigDTO> detectionConfigDTOs = this.detectionConfigDAO.findByPredicate(
        Predicate.EQ("name", name));
    if (!detectionConfigDTOs.isEmpty()){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection name already exist: " + name )).build();
    }
    Map<String, String> responseMessage = new HashMap<>();
    DetectionConfigDTO detectionConfig =
        buildDetectionConfigFromYaml(startTime, endTime, yamlConfig, null, responseMessage);
    if (detectionConfig == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    detectionConfig.setYaml(payload);
    Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
    Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");
    return Response.ok(detectionConfig).build();
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
  public Response editDetectionPipeline(
      @ApiParam("yaml config") String payload,
      @ApiParam("the detection config id to edit") @PathParam("id") long id,
      @ApiParam("tuning window start time for tunable components")  @QueryParam("startTime") long startTime,
      @ApiParam("tuning window end time for tunable components") @QueryParam("endTime") long endTime) {
    if (StringUtils.isBlank(payload)){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "empty payload")).build();
    }
    Map<String, Object> yamlConfig;
    try {
      yamlConfig = (Map<String, Object>) this.yaml.load(payload);
    } catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("message", "detection yaml parsing error, " + e.getMessage())).build();
    }

    Map<String, String> responseMessage = new HashMap<>();
    // retrieve id if detection config already exists
    DetectionConfigDTO existingDetectionConfig = this.detectionConfigDAO.findById(id);
    if (existingDetectionConfig == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    DetectionConfigDTO detectionConfig =
        buildDetectionConfigFromYaml(startTime, endTime, yamlConfig, existingDetectionConfig, responseMessage);
    if (detectionConfig == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    detectionConfig.setYaml(payload);
    Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
    Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");
    return Response.ok(detectionConfig).build();
  }

  @POST
  @Path("/notification")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Create a notification group using a YAML config")
  @SuppressWarnings("unchecked")
  public Response createDetectionAlertConfig(
      @ApiParam("payload") String yamlAlertConfig) {
    Map<String, String> responseMessage = new HashMap<>();
    Long detectionAlertConfigId;
    try {
      DetectionAlertConfigDTO alertConfig = createDetectionAlertConfig(yamlAlertConfig, responseMessage);
      if (alertConfig == null) {
        return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
      }

      detectionAlertConfigId = this.detectionAlertConfigDAO.save(alertConfig);
      if (detectionAlertConfigId == null) {
        responseMessage.put("message", "Failed to save the detection alert config.");
        responseMessage.put("more-info", "Check for potential DB issues. YAML alert config = " + yamlAlertConfig);
        return Response.serverError().entity(responseMessage).build();
      }
    } catch (Exception e) {
      responseMessage.put("message", "Failed to save the detection alert config.");
      responseMessage.put("more-info", "Exception = " + e);
      return Response.serverError().entity(responseMessage).build();
    }

    responseMessage.put("message", "The YAML alert config was saved successfully.");
    responseMessage.put("more-info", "Record saved with id " + detectionAlertConfigId);
    return Response.ok().entity(responseMessage).build();
  }


  @SuppressWarnings("unchecked")
  public DetectionAlertConfigDTO createDetectionAlertConfig(String yamlAlertConfig, Map<String, String> responseMessage ) {
    if (!alertValidator.validateYAMLConfig(yamlAlertConfig, responseMessage)) {
      return null;
    }

    TreeMap<String, Object> newAlertConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newAlertConfigMap.putAll((Map<String, Object>) this.yaml.load(yamlAlertConfig));

    // Check if a subscription group with the name already exists
    String subsGroupName = MapUtils.getString(newAlertConfigMap, PROP_SUBS_GROUP_NAME);
    if (StringUtils.isEmpty(subsGroupName)) {
      responseMessage.put("message", "Subscription group name field cannot be left empty.");
      return null;
    }
    List<DetectionAlertConfigDTO> alertConfigDTOS = this.detectionAlertConfigDAO
        .findByPredicate(Predicate.EQ("name", MapUtils.getString(newAlertConfigMap, PROP_SUBS_GROUP_NAME)));
    if (!alertConfigDTOS.isEmpty()) {
      responseMessage.put("message", "Subscription group name is already taken. Please use a different name.");
      return null;
    }

    // Translate config from YAML to detection alert config (JSON)
    DetectionAlertConfigDTO alertConfig;
    try {
      alertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);
    } catch (Exception e){
      responseMessage.put("message", e.getMessage());
      return null;
    }
    alertConfig.setYaml(yamlAlertConfig);

    // Validate the config before saving it
    if (!alertValidator.validateConfig(alertConfig, responseMessage)) {
      return null;
    }

    return alertConfig;
  }

  @PUT
  @Path("/notification/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Edit a notification group using a YAML config")
  @SuppressWarnings("unchecked")
  public Response updateDetectionAlertConfig(
      @ApiParam("payload") String yamlAlertConfig,
      @ApiParam("the detection alert config id to edit") @PathParam("id") long id) {
    Map<String, String> responseMessage = new HashMap<>();
    try {
      DetectionAlertConfigDTO alertDTO = this.detectionAlertConfigDAO.findById(id);
      DetectionAlertConfigDTO updatedAlertConfig = updateDetectionAlertConfig(alertDTO, yamlAlertConfig, responseMessage);
      if (updatedAlertConfig == null) {
        return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
      }

      int detectionAlertConfigId = this.detectionAlertConfigDAO.update(updatedAlertConfig);
      if (detectionAlertConfigId <= 0) {
        responseMessage.put("message", "Failed to update the detection alert config.");
        responseMessage.put("more-info", "Zero records updated. Check for DB issues. YAML config = " + yamlAlertConfig);
        return Response.serverError().entity(responseMessage).build();
      }
    } catch (Exception e) {
      responseMessage.put("message", "Failed to update the detection alert config.");
      responseMessage.put("more-info", "Exception = " + e);
      return Response.serverError().entity(responseMessage).build();
    }

    responseMessage.put("message", "The YAML alert config was updated successfully.");
    return Response.ok().entity(responseMessage).build();
  }

  public DetectionAlertConfigDTO updateDetectionAlertConfig(DetectionAlertConfigDTO oldAlertConfig, String yamlAlertConfig,
      Map<String,String> responseMessage) {
    if (oldAlertConfig == null) {
      responseMessage.put("message", "Cannot find subscription group");
      return null;
    }

    if (!alertValidator.validateYAMLConfig(yamlAlertConfig, responseMessage)) {
      return null;
    }

    TreeMap<String, Object> newAlertConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    newAlertConfigMap.putAll((Map<String, Object>) this.yaml.load(yamlAlertConfig));

    // Search for the detection alert config's reference in the db
    String subsGroupName = MapUtils.getString(newAlertConfigMap, PROP_SUBS_GROUP_NAME);
    if (StringUtils.isEmpty(subsGroupName)) {
      responseMessage.put("message", "Subscription group name field cannot be left empty.");
      return null;
    }

    DetectionAlertConfigDTO newAlertConfig;
    try {
      newAlertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);
    } catch (Exception e){
      responseMessage.put("message", e.getMessage());
      return null;
    }

    // Translate config from YAML to detection alert config (JSON)
    DetectionAlertConfigDTO updatedAlertConfig = updateDetectionAlertConfig(oldAlertConfig, newAlertConfig);
    updatedAlertConfig.setYaml(yamlAlertConfig);

    // Validate before updating the config
    if (!alertValidator.validateUpdatedConfig(updatedAlertConfig, oldAlertConfig, responseMessage)) {
      return null;
    }

    return updatedAlertConfig;
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

  /**
   * Update the existing {@code oldAlertConfig} with the new {@code newAlertConfig}
   *
   * Update all the fields except the vector clocks and high watermark. The clocks and watermarks
   * are managed by the platform. They shouldn't be reset by the user.
   */
  public DetectionAlertConfigDTO updateDetectionAlertConfig(DetectionAlertConfigDTO oldAlertConfig,
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
}
