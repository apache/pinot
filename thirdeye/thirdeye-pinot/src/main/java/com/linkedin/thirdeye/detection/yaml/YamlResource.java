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
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static final String PROP_NAME = "detectionName";
  private static final String PROP_TYPE = "type";
  private static final String PROP_DETECTION_CONFIG_ID = "detectionConfigIds";


  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final YamlDetectionTranslatorLoader translatorLoader;
  private final YamlDetectionAlertConfigTranslator alertConfigTranslator;
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
    this.alertConfigTranslator = new YamlDetectionAlertConfigTranslator();
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
          this.detectionConfigDAO.findByPredicate(Predicate.EQ("name", MapUtils.getString(yamlConfig, PROP_NAME)));
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
   translate alert yaml to detection alert config
   */
  private DetectionAlertConfigDTO getDetectionAlertConfig(Map<String, Object> alertYaml, Long detectionConfigId) {
    Preconditions.checkArgument(alertYaml.containsKey(PROP_NAME), "alert name missing");

    // try to retrieve existing alert config
    List<DetectionAlertConfigDTO> existingAlertConfigDTOs =
        this.detectionAlertConfigDAO.findByPredicate(Predicate.EQ("name", MapUtils.getString(alertYaml, PROP_NAME)));

    if (existingAlertConfigDTOs.isEmpty()) {
      // if alert does not exist, create a new alerter
      return this.alertConfigTranslator.generateDetectionAlertConfig(alertYaml, Collections.singletonList(detectionConfigId), new HashMap<>());
    } else {
      // get existing detection alerter
      DetectionAlertConfigDTO existingAlertConfigDTO = existingAlertConfigDTOs.get(0);
      if (alertYaml.containsKey(PROP_TYPE)) {
        // if alert Yaml contains alert configuration, update existing alert config properties
        Set<Long> detectionConfigIds =
            new HashSet(ConfigUtils.getLongs(existingAlertConfigDTO.getProperties().get(PROP_DETECTION_CONFIG_ID)));
        detectionConfigIds.add(detectionConfigId);
        DetectionAlertConfigDTO alertConfigDTO =
            this.alertConfigTranslator.generateDetectionAlertConfig(alertYaml, detectionConfigIds, existingAlertConfigDTO.getVectorClocks());
        alertConfigDTO.setId(existingAlertConfigDTO.getId());
        alertConfigDTO.setHighWaterMark(existingAlertConfigDTO.getHighWaterMark());
        return alertConfigDTO;
      } else {
        // Yaml does not contains alert config properties, add the detection pipeline to a existing alerter
        Map<Long, Long> existingVectorClocks = existingAlertConfigDTO.getVectorClocks();
        if (!existingVectorClocks.containsKey(detectionConfigId)) {
          existingVectorClocks.put(detectionConfigId, 0L);
        }
        Set<Long> detectionConfigIds =
            new HashSet(ConfigUtils.getList(existingAlertConfigDTO.getProperties().get(PROP_DETECTION_CONFIG_ID)));
        detectionConfigIds.add(detectionConfigId);
        existingAlertConfigDTO.getProperties().put(PROP_DETECTION_CONFIG_ID, detectionConfigIds);
        return existingAlertConfigDTO;
      }
    }
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
}
