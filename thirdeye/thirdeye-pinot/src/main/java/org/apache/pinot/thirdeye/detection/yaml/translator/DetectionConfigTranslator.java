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

package org.apache.pinot.thirdeye.detection.yaml.translator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.yaml.translator.builder.DetectionConfigPropertiesBuilder;
import org.apache.pinot.thirdeye.detection.yaml.translator.builder.DataQualityPropertiesBuilder;
import org.apache.pinot.thirdeye.detection.yaml.translator.builder.DetectionPropertiesBuilder;


/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties.
 * This pipeline supports multiple detection algorithm running in OR relationship.
 * Supports running multiple filter rule running in AND relationship.
 * Conceptually, the pipeline has the following structure.
 *
 *         +-----------+
 *         | Dimension |
 *         |Exploration|
 *         +-----+-----+
 *               |
 *         +-----v-----+
 *         |Dimension  |
 *         |Filter     |
 *       +-------------+--+
 *       |                |
 *       |                |
 * +-----v----+     +-----v----+
 * | Rule     |     | Algorithm|
 * | detection|     | Detection|
 * +----------+     +-----+----+
 *       |                |
 * +-----v----+     +-----v----+
 * |Rule      |     |Algorithm |
 * |  Merger  |     |  Merger  |
 * +-----+----+     +-----+----+
 *       |                |
 *       v                v
 * +-----+----+     +-----+----+
 * |Rule      |     |Algorithm |
 * |Filters   |     |Filters   |
 * +-----+----+     +-----+----+
 *       |                |
 *       v                v
 * +-----+----+     +-----+----+
 * |Rule      |     |Algorithm |
 * |Labelers  |     |Labelers  |
 * +-----+----+     +-----+----+
 *       +-------+--------+
 *               |
 *         +-----v-----+
 *         |   Merger  |
 *         |           |
 *         +-----+-----+
 *               |
 *               |
 *         +-----v-----+
 *         |  Grouper  |
 *         |           |
 *         +-----------+
 *
 *
 * This translator translates a yaml that describes the above detection flow
 * to the flowing wrapper structure to be execute in the detection pipeline.
 * +-------------------------------------------------+
 * |  Merger                                         |
 * |  +-------------------------------------------+  |
 * |  |       Dimension Exploration & Filter      |  |
 * |  |                                           |  |
 * |  |  +-------------------------------------+  |  |
 * |  |  |            Labelers                 |  |  |
 * |  |  | +---------------------------------+ |  |  |
 * |  |  | |          Filters                | |  |  |
 * |  |  | |  +--------Merger--------------+ | |  |  |
 * |  |  | |  |  Rule detections           | | |  |  |
 * |  |  | |  +----------------------------+ | |  |  |
 * |  |  | +---------------------------------+ |  |  |
 * |  |  +-------------------------------------+  |  |
 * |  |                                           |  |
 * |  |  +-------------------------------------+  |  |
 * |  |  |               Labelers              |  |  |
 * |  |  | +---------------------------------+ |  |  |
 * |  |  | |              Filters            | |  |  |
 * |  |  | | | +---------Merger------------+ | |  |  |
 * |  |  | | | | Algorithm detection       | | |  |  |
 * |  |  | | | +---------------------------+ | |  |  |
 * |  |  | +---------------------------------+ |  |  |
 * |  |  +-------------------------------------+  |  |
 * |  |                                           |  |
 * |  +-------------------------------------------+  |
 * +-------------------------------------------------+
 *
 */
public class DetectionConfigTranslator extends ConfigTranslator<DetectionConfigDTO, DetectionConfigValidator> {
  public static final String PROP_SUB_ENTITY_NAME = "subEntityName";

  static final String PROP_CRON = "cron";
  static final String PROP_TYPE = "type";
  static final String PROP_NAME = "name";
  static final String PROP_LAST_TIMESTAMP = "lastTimestamp";

  private static final String PROP_DETECTION_NAME = "detectionName";
  private static final String PROP_DESC_NAME = "description";
  private static final String PROP_ACTIVE = "active";
  private static final String PROP_OWNERS = "owners";

  static final String COMPOSITE_ALERT = "COMPOSITE_ALERT";
  static final String METRIC_ALERT = "METRIC_ALERT";

  private DataProvider dataProvider;
  private DetectionConfigPropertiesBuilder detectionTranslatorBuilder;
  private DetectionConfigPropertiesBuilder dataQualityTranslatorBuilder;
  private DetectionMetricAttributeHolder metricAttributesMap;

  public DetectionConfigTranslator(String yamlConfig, DataProvider provider) {
    this(yamlConfig, provider, new DetectionConfigValidator(provider));
  }

  public DetectionConfigTranslator(String yamlConfig, DataProvider provider, DetectionConfigValidator validator) {
    super(yamlConfig, validator);
    this.dataProvider = provider;
    this.metricAttributesMap = new DetectionMetricAttributeHolder(provider);
    this.detectionTranslatorBuilder = new DetectionPropertiesBuilder(metricAttributesMap, provider);
    this.dataQualityTranslatorBuilder = new DataQualityPropertiesBuilder(metricAttributesMap, provider);
  }

  @Override
  DetectionConfigDTO translateConfig() {
    Map<String, Object> yamlConfigMap = ConfigUtils.getMap(this.yaml.load(yamlConfig));

    // Hack to support 'detectionName' attribute at root level and 'name' attribute elsewhere
    // We consistently use 'name' as a convention to define the sub-alerts. However, at the root
    // level, as a convention, we will use 'detectionName' which defines the name of the complete alert.
    String alertName = MapUtils.getString(yamlConfigMap, PROP_DETECTION_NAME);
    yamlConfigMap.put(PROP_NAME, alertName);

    // By default if 'type' is not specified, we assume it as a METRIC_ALERT
    yamlConfigMap.putIfAbsent(PROP_TYPE, METRIC_ALERT);

    // Translate config depending on the type (METRIC_ALERT OR COMPOSITE_ALERT)
    Map<String, Object> detectionProperties;
    Map<String, Object> qualityProperties;
    String cron;
    if (yamlConfigMap.get(PROP_TYPE).equals(COMPOSITE_ALERT)) {
      detectionProperties = detectionTranslatorBuilder.buildCompositeAlertProperties(yamlConfigMap);
      qualityProperties = dataQualityTranslatorBuilder.buildCompositeAlertProperties(yamlConfigMap);

      // TODO: discuss strategy for default cron
      Preconditions.checkArgument(yamlConfigMap.containsKey(PROP_CRON), "Missing property (" + PROP_CRON + ") in alert");
      cron = MapUtils.getString(yamlConfigMap, PROP_CRON);
    } else {
      detectionProperties = detectionTranslatorBuilder.buildMetricAlertProperties(yamlConfigMap);
      qualityProperties = dataQualityTranslatorBuilder.buildMetricAlertProperties(yamlConfigMap);
      cron = metricAttributesMap.fetchCron(yamlConfigMap);
    }

    return generateDetectionConfig(yamlConfigMap, detectionProperties, qualityProperties, cron);
  }

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  private DetectionConfigDTO generateDetectionConfig(Map<String, Object> yamlConfigMap,
      Map<String, Object> detectionProperties, Map<String, Object> qualityProperties, String cron) {
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfigMap, PROP_DETECTION_NAME));
    config.setDescription(MapUtils.getString(yamlConfigMap, PROP_DESC_NAME));
    config.setDescription(MapUtils.getString(yamlConfigMap, PROP_DESC_NAME));
    List<String> owners = ConfigUtils.getList(yamlConfigMap.get(PROP_OWNERS));
    owners.replaceAll(String::trim);
    config.setOwners(new ArrayList<>(new HashSet<>(owners)));

    /*
     * The lastTimestamp value is used as a checkpoint/high watermark for onboarding data.
     * This implies that data entries post this timestamp will be processed for
     * anomalies.
     */
    config.setLastTimestamp(longValue(yamlConfigMap.get(PROP_LAST_TIMESTAMP)));

    config.setProperties(detectionProperties);
    config.setDataQualityProperties(qualityProperties);
    config.setComponentSpecs(this.metricAttributesMap.getAllComponents());
    config.setCron(cron);
    config.setActive(MapUtils.getBooleanValue(yamlConfigMap, PROP_ACTIVE, true));
    config.setYaml(yamlConfig);

    //TODO: data-availability trigger is only enabled for detections running on PINOT daily dataset only
    List<DatasetConfigDTO> datasetConfigs = this.metricAttributesMap.getAllDatasets();
    if (MapUtils.getString(yamlConfigMap, PROP_CRON) == null
        && datasetConfigs.stream().allMatch(c -> c.bucketTimeGranularity().getUnit().equals(TimeUnit.DAYS))
        && datasetConfigs.stream().allMatch(c -> c.getDataSource().equals(PinotThirdEyeDataSource.class.getSimpleName()))) {
      config.setDataAvailabilitySchedule(true);
    }
    return config;
  }

  private static long longValue(final Object o) {
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    return -1;
  }
}
