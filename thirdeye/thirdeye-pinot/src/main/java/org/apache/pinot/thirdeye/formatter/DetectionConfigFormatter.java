/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.formatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.health.DetectionHealth;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.yaml.snakeyaml.Yaml;

import static org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator.*;
import static org.apache.pinot.thirdeye.detection.yaml.translator.builder.DetectionConfigPropertiesBuilder.*;


/**
 * The detection config formatter
 */
public class DetectionConfigFormatter implements DTOFormatter<DetectionConfigDTO> {
  static final String ATTR_ID = "id";
  static final String ATTR_CREATED_BY = "createdBy";
  static final String ATTR_UPDATED_BY = "updatedBy";
  static final String ATTR_NAME = "name";
  static final String ATTR_DESCRIPTION = "description";
  static final String ATTR_LAST_TIMESTAMP = "lastTimestamp";
  static final String ATTR_YAML = "yaml";
  static final String ATTR_METRIC_URNS = "metricUrns";
  static final String ATTR_IS_ACTIVE = "active";
  static final String ATTR_ALERT_DETAILS_WINDOW_SIZE = "alertDetailsDefaultWindowSize";
  static final String ATTR_DATASET_NAME = "datasetNames";
  static final String ATTR_METRIC = "metric";
  static final String ATTR_FILTERS = "filters";
  static final String ATTR_DIMENSION_EXPLORE = "dimensionExploration";
  static final String ATTR_RULES = "rules";
  static final String ATTR_GRANULARITY = "monitoringGranularity";
  static final String ATTR_HEALTH = "health";

  private static final String PROP_METRIC_URNS_KEY = "metricUrn";
  private static final String PROP_NESTED_METRIC_URNS_KEY = "nestedMetricUrns";
  private static final String PROP_NESTED_PROPERTIES_KEY = "nested";
  private static final String PROP_MONITORING_GRANULARITY = "monitoringGranularity";
  private static final String PROP_BUCKET_PERIOD = "bucketPeriod";

  private static final long DEFAULT_PRESENTING_WINDOW_SIZE_MINUTELY = TimeUnit.HOURS.toMillis(48);
  private static final long DEFAULT_PRESENTING_WINDOW_SIZE_DAILY = TimeUnit.DAYS.toMillis(30);
  private static final TimeGranularity DEFAULT_SHOW_GRANULARITY = new TimeGranularity(1, TimeUnit.DAYS);

  private static final String METRIC_ALERT_TYPE = "METRIC_ALERT";
  private static final String COMPOSITE_ALERT_TYPE = "COMPOSITE_ALERT";
  private static final String ALGORITHM_TYPE = "ALGORITHM";
  private static final String CONFIGURATION = "configuration";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EvaluationManager evaluationDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final TaskManager taskDAO;
  private final Yaml yaml;

  public DetectionConfigFormatter(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    this.datasetDAO = datasetDAO;
    this.metricDAO = metricDAO;
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.yaml = new Yaml();
  }

  @Override
  public Map<String, Object> format(DetectionConfigDTO config) {
    Map<String, Object> output = new HashMap<>();
    output.put(ATTR_ID, config.getId());
    output.put(ATTR_IS_ACTIVE, config.isActive());
    output.put(ATTR_CREATED_BY, config.getCreatedBy());
    output.put(ATTR_UPDATED_BY, config.getUpdatedBy());
    output.put(ATTR_NAME, config.getName());
    output.put(ATTR_DESCRIPTION, config.getDescription());
    output.put(ATTR_YAML, config.getYaml());
    output.put(ATTR_LAST_TIMESTAMP, config.getLastTimestamp());
    output.put(ATTR_HEALTH, getDetectionHealth(config));

    Set<String> metricUrns = extractMetricUrnsFromProperties(config.getProperties());
    Set<String> metrics = new HashSet<>();

    Map<String, MetricConfigDTO> metricUrnToMetricDTOs = new HashMap<>();
    for (String metricUrn : metricUrns) {
      metricUrnToMetricDTOs.put(metricUrn, getMetricConfigForMetricUrn(metricUrn));
    }

    Map<String, Object> yamlObject;
    // synchronize on yaml parser because it is not thread safe
    synchronized (this.yaml) {
      yamlObject = ConfigUtils.getMap(this.yaml.load(config.getYaml()));
    }
    Map<String, DatasetConfigDTO> metricUrnToDatasets = metricUrns.stream().collect(Collectors.toMap(metricUrn -> metricUrn,
      metricUrn -> getDatasetForMetricUrn(metricUrn, metricUrnToMetricDTOs), (d1, d2) -> d1));

    // the metric urns monitored by this detection config
    output.put(ATTR_METRIC_URNS, metricUrns);
    // get the granularity for the detection
    List<TimeGranularity> granularities = getGranularitiesForConfig(config, metricUrnToDatasets);
    output.put(ATTR_GRANULARITY, granularities.stream().map(TimeGranularity::toAggregationGranularityString).distinct().collect(Collectors.toList()));
    // the default window size of the alert details page
    output.put(ATTR_ALERT_DETAILS_WINDOW_SIZE, getAlertDetailsDefaultWindowSize(granularities));
    output.put(ATTR_METRIC, extractMetric(yamlObject, metrics));
    output.put(ATTR_FILTERS, yamlObject.get(PROP_FILTERS));
    output.put(ATTR_DIMENSION_EXPLORE, yamlObject.get(PROP_DIMENSION_EXPLORATION));
    output.put(ATTR_DATASET_NAME, getDatasetDisplayNames(metricUrnToDatasets, yamlObject));
    output.put(ATTR_RULES, yamlObject.get(PROP_RULES));
    return output;
  }

  private Set<String> extractMetric(Map<String, Object> yamlObject, Set<String> metrics) {
    if (yamlObject.get(PROP_TYPE) != null && COMPOSITE_ALERT_TYPE.equals(yamlObject.get(PROP_TYPE).toString())) {
      if (yamlObject.get(PROP_ALERTS) != null) {
        List<Map<String,Object>> nestedAlerts = (List<Map<String, Object>>) yamlObject.get(PROP_ALERTS);
        nestedAlerts.forEach(alert -> extractMetric(alert, metrics));
        return metrics;
      } else {
        throw new RuntimeException("Invalid alert config: composite alert without the alerts clause.");
      }
    } else {
      metrics.add(yamlObject.get(PROP_METRIC).toString());
      return metrics;
    }
  }

  /**
   * Extract the list of metric urns in the detection config properties
   * @param properties the detection config properties
   * @return the list of metric urns
   */
  public static Set<String> extractMetricUrnsFromProperties(Map<String, Object> properties) {
    Set<String> metricUrns = new HashSet<>();
    if (properties.containsKey(PROP_METRIC_URNS_KEY)) {
      metricUrns.add((String)properties.get(PROP_METRIC_URNS_KEY));
    }
    if (properties.containsKey(PROP_NESTED_METRIC_URNS_KEY)) {
      metricUrns.addAll(ConfigUtils.getList(properties.get(PROP_NESTED_METRIC_URNS_KEY)));
    }
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get(PROP_NESTED_PROPERTIES_KEY));
    // extract the metric urns recursively from the nested properties
    for (Map<String, Object> nestedProperty : nestedProperties) {
      metricUrns.addAll(extractMetricUrnsFromProperties(nestedProperty));
    }
    return metricUrns;
  }

  private DetectionHealth getDetectionHealth(DetectionConfigDTO config) {
    // Return the stored detection health when it is available in the DetectionConfig.
    if (!Objects.isNull(config.getHealth())) {
      return config.getHealth();
    }
    return DetectionHealth.unknown();
  }

  /**
   * Get the display names for the datasets. Show the display name if it is available in the dataset. Otherwize, use the dataset name.
   * @param metricUrnToDatasets the map of detection config keyed buy metric urn
   * @return the list of dataset names to show in UI
   */
  private List<String> getDatasetDisplayNames(Map<String, DatasetConfigDTO> metricUrnToDatasets, Map<String, Object> yamlObject) {
    return metricUrnToDatasets.values()
        .stream()
        .map(datasetConfigDTO -> {
          String datasetName = datasetConfigDTO.getName();
          return StringUtils.isNotBlank(datasetName) ? datasetName : MapUtils.getString(yamlObject, PROP_DATASET);
        })
        .distinct()
        .collect(Collectors.toList());
  }

  private DatasetConfigDTO getDatasetForMetricUrn(String metricUrn,
      Map<String, MetricConfigDTO> metricUrnToMetricDTOs) {
    MetricConfigDTO metricConfig = metricUrnToMetricDTOs.get(metricUrn);
    if (metricConfig != null) {
      DatasetConfigDTO datasetConfig = this.datasetDAO.findByDataset(metricConfig.getDataset());
      if (datasetConfig != null) {
        return datasetConfig;
      }
    }
    return new DatasetConfigDTO();
  }

  private MetricConfigDTO getMetricConfigForMetricUrn(String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    return this.metricDAO.findById(me.getId());
  }

  private List<TimeGranularity> getGranularitiesForConfig(DetectionConfigDTO config, Map<String, DatasetConfigDTO> metricUrnToDatasets) {
    try {
      // first try to get the granularities in config properties
      List<TimeGranularity> granularities = getDetectionConfigMonitoringGranularities(config);
      // if monitoring granularities is not set, use the metric granularity to decide the default window
      if (granularities.isEmpty()) {
        granularities = metricUrnToDatasets.keySet().stream().map(metricUrn -> metricUrnToDatasets.get(metricUrn).bucketTimeGranularity()).collect(Collectors.toList());
      }
      return granularities;
    } catch (Exception e) {
      // return the default granularity if the dataset is not available. Most likely caused by the dataset is deleted.
      return Collections.singletonList(DEFAULT_SHOW_GRANULARITY);
    }
  }

  /**
   * Generate the default window size for presenting in ThirdEye alert details page.
   * @param granularities list of granularities
   * @return the window size in milliseconds
   */
  private long getAlertDetailsDefaultWindowSize(List<TimeGranularity> granularities) {
    List<Long> windowSizes =
        granularities.stream().map(this::getDefaultWindowSizeForGranularity).collect(Collectors.toList());
    // show the minimal of all window sizes for UI responsiveness
    return Collections.min(windowSizes);
  }

  private List<TimeGranularity> getDetectionConfigMonitoringGranularities(DetectionConfigDTO config) {
    List<TimeGranularity> monitoringGranularities = new ArrayList<>();
    for (Map.Entry<String, Object> entry : config.getComponentSpecs().entrySet()) {
      Map<String, Object> specs = (Map<String, Object>) entry.getValue();
      if (entry.getKey().equals(ALGORITHM_TYPE)) {
        extractTimeGranularitiesFromAlgorithmSpecs(specs, PROP_BUCKET_PERIOD).ifPresent(monitoringGranularities::add);
      } else if (specs.containsKey(PROP_MONITORING_GRANULARITY)) {
        monitoringGranularities.add(TimeGranularity.fromString(MapUtils.getString(specs, (PROP_MONITORING_GRANULARITY))));
      }
    }
    return monitoringGranularities;
  }

  /**
   * extract the detection granularity for ALGORITHM detector types
   * @param specs the specs for the ALGORITHM detectors
   * @return the granularity
   */
  private Optional<TimeGranularity> extractTimeGranularitiesFromAlgorithmSpecs(Map<String, Object> specs, String bucketPeriodFieldName) {
    String bucketPeriod = String.valueOf(MapUtils.getMap(specs, CONFIGURATION).get(bucketPeriodFieldName));
    Period p = Period.parse(bucketPeriod);
    PeriodType periodType = p.getPeriodType();
    if (PeriodType.days().equals(periodType)) {
      return  Optional.of(new TimeGranularity(p.getDays(), TimeUnit.DAYS));
    } else if (PeriodType.hours().equals(periodType)) {
      return Optional.of(new TimeGranularity(p.getHours(), TimeUnit.HOURS));
    } else if (PeriodType.minutes().equals(periodType)) {
      return Optional.of(new TimeGranularity(p.getMinutes(), TimeUnit.MINUTES));
    }
    return Optional.empty();
  }


  private long getDefaultWindowSizeForGranularity(TimeGranularity granularity) {
    TimeUnit unit = granularity.getUnit();
    if (unit == TimeUnit.MINUTES || unit == TimeUnit.HOURS) {
      return DEFAULT_PRESENTING_WINDOW_SIZE_MINUTELY;
    } else {
      return DEFAULT_PRESENTING_WINDOW_SIZE_DAILY;
    }
  }
}
