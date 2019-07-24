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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  private static final String PROP_NESTED_METRIC_URNS_KEY = "nestedMetricUrns";
  private static final String PROP_NESTED_PROPERTIES_KEY = "nested";
  private static final String PROP_MONITORING_GRANULARITY = "monitoringGranularity";
  private static final String PROP_BUCKET_PERIOD = "bucketPeriod";
  private static final String PROP_VARIABLES_BUCKET_PERIOD = "variables.bucketPeriod";

  private static final Logger LOG = LoggerFactory.getLogger(DetectionConfigFormatter.class);

  private static final long DEFAULT_PRESENTING_WINDOW_SIZE_MINUTELY = TimeUnit.HOURS.toMillis(48);
  private static final long DEFAULT_PRESENTING_WINDOW_SIZE_DAILY = TimeUnit.DAYS.toMillis(30);
  private static final String ALGORITHM_TYPE = "ALGORITHM";
  private static final String MIGRATED_ALGORITHM_TYPE = "MIGRATED_ALGORITHM";
  private static final String CONFIGURATION = "configuration";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;

  public DetectionConfigFormatter(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    this.datasetDAO = datasetDAO;
    this.metricDAO = metricDAO;
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
    List<String> metricUrns = extractMetricUrnsFromProperties(config.getProperties());
    // the metric urns monitored by this detection config
    output.put(ATTR_METRIC_URNS, metricUrns);
    // the default window size of the alert details page
    output.put(ATTR_ALERT_DETAILS_WINDOW_SIZE, getAlertDetailsDefaultWindowSize(config, metricUrns));
    return output;
  }

  /**
   * Extract the list of metric urns in the detection config properties
   * @param properties the detection config properties
   * @return the list of metric urns
   */
  private List<String> extractMetricUrnsFromProperties(Map<String, Object> properties) {
    List<String> metricUrns = new ArrayList<>();
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

  /**
   * Generate the default window size for presenting in ThirdEye alert details page.
   * @param config the detection config
   * @param metricUrns the list of metric urns
   * @return the window size in milliseconds
   */
  private long getAlertDetailsDefaultWindowSize(DetectionConfigDTO config, List<String> metricUrns) {
    try {
      // first try to get the granularities in config properties
      List<TimeGranularity> granularities = getMonitoringGranularities(config);
      // if monitoring granularities is not set, use the metric granularity to decide the default window
      if (granularities.isEmpty()) {
        granularities = metricUrns.stream().map(this::getGranularityForMetricUrn).collect(Collectors.toList());
      }
      List<Long> windowSizes =
          granularities.stream().map(this::getDefaultWindowSizeForGranularity).collect(Collectors.toList());
      // show the minimal of all window sizes for UI responsiveness
      return Collections.min(windowSizes);
    } catch (Exception e) {
      LOG.warn("Exception thrown when getting granularities for detection config {}, use default presenting window size", config.getId(), e);
      return DEFAULT_PRESENTING_WINDOW_SIZE_DAILY;
    }
  }

  private List<TimeGranularity> getMonitoringGranularities(DetectionConfigDTO config) {
    List<TimeGranularity> monitoringGranularities = new ArrayList<>();
    for (Map.Entry<String, Object> entry : config.getComponentSpecs().entrySet()) {
      Map<String, Object> specs = (Map<String, Object>) entry.getValue();
      if (entry.getKey().equals(ALGORITHM_TYPE)) {
        extractTimeGranularitiesFromAlgorithmSpecs(specs, PROP_BUCKET_PERIOD).ifPresent(monitoringGranularities::add);
      } else if (entry.getKey().equals(MIGRATED_ALGORITHM_TYPE)) {
        extractTimeGranularitiesFromAlgorithmSpecs(specs, PROP_VARIABLES_BUCKET_PERIOD).ifPresent(monitoringGranularities::add);
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

  private TimeGranularity getGranularityForMetricUrn(String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    return this.datasetDAO.findByDataset(this.metricDAO.findById(me.getId()).getDataset()).bucketTimeGranularity();
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
