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

package com.linkedin.thirdeye.detection;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import static com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy.*;


/**
 * The Detection migration resource.
 */
@Path("/migrate")
public class DetectionMigrationResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DetectionMigrationResource.class);
  private static final String PROP_WINDOW_DELAY = "windowDelay";
  private static final String PROP_WINDOW_DELAY_UNIT = "windowDelayUnit";

  private final LegacyAnomalyFunctionTranslator translator;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DetectionConfigManager detectionConfigDAO;
  private final Yaml yaml;

  /**
   * Instantiates a new Detection migration resource.
   *
   * @param anomalyFunctionFactory the anomaly function factory
   */
  public DetectionMigrationResource(MetricConfigManager metricConfigDAO, AnomalyFunctionManager anomalyFunctionDAO,
      DetectionConfigManager detectionConfigDAO, AnomalyFunctionFactory anomalyFunctionFactory,
      AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.detectionConfigDAO = detectionConfigDAO;
    this.translator = new LegacyAnomalyFunctionTranslator(metricConfigDAO, anomalyFunctionFactory, alertFilterFactory);
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    this.yaml = new Yaml(options);
  }

  @Path("/stats")
  @GET
  public void getAnomalyFunctionStats() {
    List<AnomalyFunctionDTO> anomalyFunctions = this.anomalyFunctionDAO.findAll();
    long mergeConfigCount = 0;
    long functionDimensionsMergeStrategyCount = 0;
    long averageThresholdDataFilterCount = 0;
    long datafilterCount = 0;
    long swiFilterCount = 0;
    long twoSideSWICount = 0;
    long otherFilter = 0;

    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctions) {
      if (!anomalyFunctionDTO.getIsActive()) {
        continue;
      }
      if (anomalyFunctionDTO.getAnomalyMergeConfig() != null ) {
        if (anomalyFunctionDTO.getAnomalyMergeConfig().getMergeStrategy() == FUNCTION_DIMENSIONS){
          functionDimensionsMergeStrategyCount++;
        }
        mergeConfigCount++;
      }
      if (anomalyFunctionDTO.getDataFilter() != null && !anomalyFunctionDTO.getDataFilter().isEmpty()) {
        if (anomalyFunctionDTO.getDataFilter().get("type").equals("average_threshold")){
          averageThresholdDataFilterCount++;
        } else {
          LOGGER.info("other data filter {}", anomalyFunctionDTO.getDataFilter().get("type"));
        }
        datafilterCount++;
      }
      if (anomalyFunctionDTO.getAlertFilter() != null){
        if(anomalyFunctionDTO.getAlertFilter().get("thresholdField") != null){
          if (!anomalyFunctionDTO.getAlertFilter().get("thresholdField").equals("impactToGlobal")) {
            otherFilter++;
          }
          swiFilterCount++;
          if (anomalyFunctionDTO.getAlertFilter().get("maxThreshold") != anomalyFunctionDTO.getAlertFilter().get("maxThreshold")){
            twoSideSWICount++;
          }
        }
      }
    }
    LOGGER.info("anomalyFunctions count", anomalyFunctions.size());
    LOGGER.info("mergeConfigCount {}", mergeConfigCount);
    LOGGER.info("functionDimensionsMergeStrategyCount {}", functionDimensionsMergeStrategyCount);
    LOGGER.info("datafilterCount {}", datafilterCount);
    LOGGER.info("averageThresholdDataFilterCount {}", averageThresholdDataFilterCount);
    LOGGER.info("swiFilterCount {}", swiFilterCount);
    LOGGER.info("twoSideSWICount {}", twoSideSWICount);
    LOGGER.info("otherFilter {}", otherFilter);
  }

  @GET
  public String migrateToYaml(@QueryParam("id") long anomalyFunctionId) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    Map<String, Object> yamlConfigs = new LinkedHashMap<>();
    yamlConfigs.put("detectionName", anomalyFunctionDTO.getFunctionName());
    yamlConfigs.put("metric", anomalyFunctionDTO.getMetric());
    yamlConfigs.put("dataset", anomalyFunctionDTO.getCollection());
    yamlConfigs.put("pipelineType", "Composite");
    if (anomalyFunctionDTO.getExploreDimensions() != null) {
      yamlConfigs.put("dimensionExploration",
          getDimensionExplorationParams(anomalyFunctionDTO));
    }
    // TODO plugin dimension filter
    yamlConfigs.put("filters",
        AnomalyDetectionInputContextBuilder.getFiltersForFunction(anomalyFunctionDTO.getFilters()).asMap());

    Map<String, Object> ruleYaml = new LinkedHashMap<>();
    ruleYaml.put("name", "myRule");

    // detection
    ruleYaml.put("detection", Collections.singletonList(
        ImmutableMap.of("type", "ALGORITHM", "params", getAlgorithmDetectorParams(anomalyFunctionDTO))));

    // filters
    Map<String, String> alertFilter = anomalyFunctionDTO.getAlertFilter();

    if (alertFilter != null && !alertFilter.isEmpty()){
      Map<String, Object> filterYaml = new LinkedHashMap<>();
      if (!alertFilter.containsKey("thresholdField")) {
        // algorithm alert filter
        filterYaml = ImmutableMap.of("type", "ALGORITHM_FILTER", "params", getAlertFilterParams(anomalyFunctionDTO));
      } else {
        // threshold filter migrate to rule filters
        // site wide impact filter migrate to rule based swi filter
        if (anomalyFunctionDTO.getAlertFilter().get("thresholdField").equals("impactToGlobal")){
          filterYaml.put("type", "SITEWIDE_IMPACT_FILTER");
          filterYaml.put("params", getSiteWideImpactFilterParams(anomalyFunctionDTO));
        }
        // weight filter migrate to rule based percentage change filter
        if (anomalyFunctionDTO.getAlertFilter().get("thresholdField").equals("weight")){
          filterYaml.put("type", "PERCENTAGE_CHANGE_FILTER");
          filterYaml.put("params", getPercentageChangeFilterParams(anomalyFunctionDTO));
        }
      }
      ruleYaml.put("filter", Collections.singletonList(filterYaml));
    }

    yamlConfigs.put("rules", Collections.singletonList(ruleYaml));

    // merger configs
    if (anomalyFunctionDTO.getAnomalyMergeConfig() != null ) {
      Map<String, Object> mergerYaml = new LinkedHashMap<>();
      if (anomalyFunctionDTO.getAnomalyMergeConfig().getMergeStrategy() == FUNCTION_DIMENSIONS){
        mergerYaml.put("maxGap", anomalyFunctionDTO.getAnomalyMergeConfig().getSequentialAllowedGap());
        mergerYaml.put("maxDuration", anomalyFunctionDTO.getAnomalyMergeConfig().getMaxMergeDurationLength());
      }
      yamlConfigs.put("merger", mergerYaml);
    }

    return this.yaml.dump(yamlConfigs);
  }

  private Map<String, Object> getDimensionExplorationParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> dimensionExploreYaml = new LinkedHashMap<>();
    dimensionExploreYaml.put("dimensions", Collections.singletonList(functionDTO.getExploreDimensions()));
    if (functionDTO.getDataFilter() != null && !functionDTO.getDataFilter().isEmpty() && functionDTO.getDataFilter().get("type").equals("average_threshold")) {
      dimensionExploreYaml.put("minValue", functionDTO.getDataFilter().get("threshold"));
    }
    return dimensionExploreYaml;
  }

  private Map<String, Object> getPercentageChangeFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    filterYamlParams.put("threshold", Math.abs(Double.valueOf(functionDTO.getAlertFilter().get("maxThreshold"))));
    filterYamlParams.put("pattern", "up_or_down");
    return filterYamlParams;
  }

  private Map<String, Object> getSiteWideImpactFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    filterYamlParams.put("threshold", Math.abs(Double.valueOf(functionDTO.getAlertFilter().get("maxThreshold"))));
    filterYamlParams.put("pattern", "up_or_down");
    filterYamlParams.put("sitewideMetricName", functionDTO.getGlobalMetric());
    filterYamlParams.put("sitewideCollection", functionDTO.getCollection());
    filterYamlParams.put("filters", AnomalyDetectionInputContextBuilder.getFiltersForFunction(functionDTO.getGlobalMetricFilters()).asMap());
    return filterYamlParams;
  }

  private Map<String, Object> getAlertFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    Map<String, Object> params = new HashMap<>();
    filterYamlParams.put("configuration", params);
    params.putAll(functionDTO.getAlertFilter());
    params.put("variables.bucketPeriod", getBucketPeriod(functionDTO));
    // TODO bucket period, timezone
    return filterYamlParams;
  }

  private String getBucketPeriod(AnomalyFunctionDTO functionDTO) {
    return new Period(TimeUnit.MILLISECONDS.convert(functionDTO.getBucketSize(), functionDTO.getWindowUnit())).toString();
  }

  private Map<String, Object> getAlgorithmDetectorParams(AnomalyFunctionDTO functionDTO) throws Exception {
    Map<String, Object> detectorYaml = new LinkedHashMap<>();
    Map<String, Object> params = new LinkedHashMap<>();
    detectorYaml.put("configuration", params);
    Properties properties = AnomalyFunctionDTO.toProperties(functionDTO.getProperties());
    for (Map.Entry<Object, Object> property : properties.entrySet()) {
      params.put((String) property.getKey(), property.getValue());
    }
    params.put("variables.bucketPeriod", getBucketPeriod(functionDTO));
    // TODO bucket period, timezone
    if (functionDTO.getWindowDelay() != 0) {
      detectorYaml.put(PROP_WINDOW_DELAY, functionDTO.getWindowDelay());
      detectorYaml.put(PROP_WINDOW_DELAY_UNIT, functionDTO.getWindowDelayUnit().toString());
    }
    return detectorYaml;
  }

  /**
   * This endpoint takes in a anomaly function Id and translate the anomaly function config to a
   * detection config of the new pipeline and then persist it in to database.
   *
   * @param anomalyFunctionId the anomaly function id
   * @return the response
   * @throws Exception the exception
   */
  @POST
  public Response migrateToDetectionPipeline(@QueryParam("id") long anomalyFunctionId, @QueryParam("name") String name,
      @QueryParam("lastTimestamp") Long lastTimestamp) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    DetectionConfigDTO config = this.translator.translate(anomalyFunctionDTO);

    if (!StringUtils.isBlank(name)) {
      config.setName(name);
    }

    config.setLastTimestamp(System.currentTimeMillis());
    if (lastTimestamp != null) {
      config.setLastTimestamp(lastTimestamp);
    }

    this.detectionConfigDAO.save(config);
    if (config.getId() == null) {
      throw new WebApplicationException(String.format("Could not migrate anomaly function %d", anomalyFunctionId));
    }

    LOGGER.info("Created detection config {} for anomaly function {}", config.getId(), anomalyFunctionDTO.getId());
    return Response.ok(config.getId()).build();
  }
}
