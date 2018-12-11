package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.linkedin.thirdeye.detection.validators.DetectionAlertConfigValidator;
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
    this.alertConfigTranslator = YamlDetectionAlertConfigTranslator.getInstance();
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
    String errorMessage;
    try {
      Preconditions.checkArgument(StringUtils.isNotBlank(payload), "Empty payload");
      Map<String, Object> yamlConfig = (Map<String, Object>) this.yaml.load(payload);

      // retrieve id if detection config already exists
      List<DetectionConfigDTO> detectionConfigDTOs =
          this.detectionConfigDAO.findByPredicate(Predicate.EQ("name", MapUtils.getString(yamlConfig, PROP_DETECTION_NAME)));
      DetectionConfigDTO existingDetectionConfig = null;
      if (!detectionConfigDTOs.isEmpty()) {
        existingDetectionConfig = detectionConfigDTOs.get(0);
      }

      YamlDetectionConfigTranslator translator = this.translatorLoader.from(yamlConfig, this.provider);
      DetectionConfigDTO detectionConfig = translator.withTrainingWindow(startTime, endTime)
          .withExistingDetectionConfig(existingDetectionConfig)
          .generateDetectionConfig();
      detectionConfig.setYaml(payload);
      validatePipeline(detectionConfig);
      Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
      Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");

      return Response.ok(detectionConfig).build();
    } catch (InvocationTargetException e){
      // exception thrown in validate pipeline via reflection
      LOG.error("Validate pipeline error", e);
      errorMessage = e.getCause().getMessage();
    } catch (Exception e) {
      LOG.error("yaml translation error", e);
      errorMessage = e.getMessage();
    }
    return Response.status(400).entity(ImmutableMap.of("status", "400", "message", errorMessage)).build();
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
    String errorMessage;
    try {
      Preconditions.checkArgument(StringUtils.isNotBlank(payload), "Empty payload");
      Map<String, Object> yamlConfig = (Map<String, Object>) this.yaml.load(payload);

      DetectionConfigDTO existingDetectionConfig = this.detectionConfigDAO.findById(id);
      Preconditions.checkArgument(existingDetectionConfig != null, "Existing detection config " + id + " not found");

      YamlDetectionConfigTranslator translator = this.translatorLoader.from(yamlConfig, this.provider);
      DetectionConfigDTO detectionConfig = translator.withTrainingWindow(startTime, endTime)
          .withExistingDetectionConfig(existingDetectionConfig)
          .generateDetectionConfig();
      detectionConfig.setYaml(payload);
      validatePipeline(detectionConfig);
      Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
      Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");

      return Response.ok(detectionConfig).build();
    } catch (InvocationTargetException e){
      // exception thrown in validate pipeline via reflection
      LOG.error("Validate pipeline error", e);
      errorMessage = e.getCause().getMessage();
    } catch (Exception e) {
      LOG.error("yaml translation error", e);
      errorMessage = e.getMessage();
    }
    return Response.status(400).entity(ImmutableMap.of("status", "400", "message", errorMessage)).build();
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
    DetectionAlertConfigDTO alertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);
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
    DetectionAlertConfigDTO newAlertConfig = this.alertConfigTranslator.translate(newAlertConfigMap);

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
   List all yaml configurations enhanced with detection config id, isActive and createBy information.
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
