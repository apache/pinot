package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
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
import com.wordnik.swagger.annotations.ApiParam;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.MapUtils;
import org.yaml.snakeyaml.Yaml;


@Path("/yaml")
public class YamlResource {
  private static final String PROP_NAME = "detectionName";
  private static final String PROP_TYPE = "type";
  private static final String PROP_DETECTION_CONFIG_ID = "detectionConfigIds";

  private static final Yaml YAML_READER = new Yaml();

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

  public YamlResource() {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.translatorLoader = new YamlDetectionTranslatorLoader();
    this.alertConfigTranslator = new YamlDetectionAlertConfigTranslator();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

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
   @return a message contains the saved detection config id & detection alert id
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response setUpDetectionPipeline(@ApiParam("payload") String payload,
      @QueryParam("startTime") long startTime, @QueryParam("endTime") long endTime) throws Exception {
    if (Strings.isNullOrEmpty(payload)) {
      throw new IllegalArgumentException("Empty Payload");
    }
    Map<String, Object> yamlConfig = (Map<String, Object>) this.YAML_READER.load(payload);

    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "missing " + PROP_NAME);
    // retrieve id if detection config already exists
    List<DetectionConfigDTO> detectionConfigDTOs =
        this.detectionConfigDAO.findByPredicate(Predicate.EQ("name", MapUtils.getString(yamlConfig, PROP_NAME)));
    DetectionConfigDTO existingDetectionConfig = null;
    if (!detectionConfigDTOs.isEmpty()) {
      existingDetectionConfig = detectionConfigDTOs.get(0);
    }

    YamlDetectionConfigTranslator translator = this.translatorLoader.from(yamlConfig, this.provider);
    DetectionConfigDTO detectionConfig;
    try{
      detectionConfig = translator.withTrainingWindow(startTime, endTime).withExistingDetectionConfig(existingDetectionConfig).generateDetectionConfig();
    } catch (Exception e) {
      return Response.status(400).entity(ImmutableMap.of("status", "400", "message", e.getMessage())).build();
    }
    detectionConfig.setYaml(payload);
    Long detectionConfigId = this.detectionConfigDAO.save(detectionConfig);
    Preconditions.checkNotNull(detectionConfigId, "Save detection config failed");

    // optionally set up an alerter for the detection pipeline
    if (yamlConfig.containsKey("alert")) {
      Map<String, Object> alertYaml = MapUtils.getMap(yamlConfig, "alert");
      DetectionAlertConfigDTO alertConfigDTO = getDetectionAlertConfig(alertYaml, detectionConfigId);
      Long detectionAlertConfigId = this.detectionAlertConfigDAO.save(alertConfigDTO);
      Preconditions.checkNotNull(detectionAlertConfigId, "Save detection alerter config failed");
    }

    return Response.ok(detectionConfig).build();
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
}
