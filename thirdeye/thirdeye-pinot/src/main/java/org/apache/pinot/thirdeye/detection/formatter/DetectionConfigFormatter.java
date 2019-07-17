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

package org.apache.pinot.thirdeye.detection.formatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The detection config formatter
 */
public class DetectionConfigFormatter implements DTOFormatter<DetectionConfigDTO> {
  private static final String ATTR_ID = "id";
  private static final String ATTR_CREATED_BY = "createdBy";
  private static final String ATTR_UPDATED_BY = "updatedBy";
  private static final String ATTR_NAME = "name";
  private static final String ATTR_DESCRIPTION = "description";
  private static final String ATTR_LAST_TIMESTAMP = "lastTimestamp";
  private static final String ATTR_YAML = "yaml";
  private static final String ATTR_METRIC_URNS = "metricUrns";
  private static final String ATTR_IS_ACTIVE = "active";
  private static final String ATTR_ALERT_DETAILS_WINDOW_SIZE = "alertDetailsDefaultWindowSize";

  private static final String PROP_NESTED_METRIC_URNS_KEY = "nestedMetricUrns";
  private static final String PROP_NESTED_PROPERTIES_KEY = "nested";
  private static final String PROP_MONITORING_GRANULARITY = "monitoringGranularity";

  private static final Logger LOG = LoggerFactory.getLogger(DetectionConfigFormatter.class);

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

  private long getAlertDetailsDefaultWindowSize(DetectionConfigDTO config, List<String> metricUrns) {
    try {
      List<TimeGranularity> granularities = getMonitoringGranularities(config);
      // if monitoring granularities is not set, use the metric granularity to decide the default window
      if (granularities.isEmpty()) {
        granularities = metricUrns.stream().map(this::getGranularityForMetricUrn).collect(Collectors.toList());
      }
      List<Long> windowSizes =
          granularities.stream().map(this::getDefaultWindowSizeForGranularity).collect(Collectors.toList());
      return Collections.min(windowSizes);
    } catch (Exception e) {
      LOG.warn("Exception thrown when getting granularities for detection config {}, use default presenting window size", config.getId(), e);
      return TimeUnit.DAYS.toMillis(30);
    }
  }

  private List<TimeGranularity> getMonitoringGranularities(DetectionConfigDTO config) {
    List<TimeGranularity> monitoringGranularities = new ArrayList<>();
    for (Object specs : config.getComponentSpecs().values()) {
      if (((Map<String, Object>) specs).containsKey(PROP_MONITORING_GRANULARITY)) {
        monitoringGranularities.add(
            TimeGranularity.fromString(MapUtils.getString((Map<String, Object>) specs, (PROP_MONITORING_GRANULARITY))));
      }
    }
    return monitoringGranularities;
  }

  private TimeGranularity getGranularityForMetricUrn(String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    return this.datasetDAO.findByDataset(this.metricDAO.findById(me.getId()).getDataset()).bucketTimeGranularity();
  }

  private long getDefaultWindowSizeForGranularity(TimeGranularity granularity) {
    TimeUnit unit = granularity.getUnit();
    if (unit == TimeUnit.MINUTES || unit == TimeUnit.HOURS) {
      return TimeUnit.HOURS.toMillis(48);
    } else {
      return TimeUnit.DAYS.toMillis(30);
    }
  }
}
