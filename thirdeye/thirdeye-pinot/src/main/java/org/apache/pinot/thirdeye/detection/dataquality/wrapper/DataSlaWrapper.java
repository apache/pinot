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

package org.apache.pinot.thirdeye.detection.dataquality.wrapper;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator.*;


/**
 * Wrapper class that is responsible for running the @{link DataSlaQualityChecker}
 */
public class DataSlaWrapper extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DataSlaWrapper.class);

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_QUALITY_CHECK = "qualityCheck";
  private static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";

  private final AnomalyDetector qualityChecker;
  private final DatasetConfigDTO dataset;
  private final MetricEntity metricEntity;
  private final MetricConfigDTO metric;
  private final String qualityCheckerKey;
  private final String entityName;
  private final String metricUrn;

  public DataSlaWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_SUB_ENTITY_NAME));
    this.entityName = MapUtils.getString(config.getProperties(), PROP_SUB_ENTITY_NAME);

    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_QUALITY_CHECK));
    this.qualityCheckerKey = DetectionUtils.getComponentKey(MapUtils.getString(config.getProperties(), PROP_QUALITY_CHECK));
    Preconditions.checkArgument(this.config.getComponents().containsKey(this.qualityCheckerKey));
    this.qualityChecker = (AnomalyDetector) this.config.getComponents().get(this.qualityCheckerKey);

    // TODO Check
    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    this.metricEntity = MetricEntity.fromURN(this.metricUrn);
    this.metric = provider.fetchMetrics(Collections.singleton(this.metricEntity.getId())).get(this.metricEntity.getId());

    MetricConfigDTO metricConfigDTO = this.provider.fetchMetrics(Collections.singletonList(this.metricEntity.getId())).get(this.metricEntity.getId());
    this.dataset = this.provider.fetchDatasets(Collections.singletonList(metricConfigDTO.getDataset()))
        .get(metricConfigDTO.getDataset());
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    //dataSlaTaskCounter.inc();

    LOG.info("Check data sla for config {} between {} and {}", config.getId(), startTime, endTime);
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    List<Interval> monitoringWindows = Collections.singletonList(new Interval(startTime, endTime, DateTimeZone.forID(dataset.getTimezone())));
    int totalWindows = monitoringWindows.size();
    int successWindows = 0;
    // The last exception of the detection windows. It will be thrown out to upper level.
    Exception lastException = null;
    for (int i = 0; i < totalWindows; i++) {
      // run detection
      Interval window = monitoringWindows.get(i);
      DetectionResult detectionResult = DetectionResult.empty();

      // TODO log
      detectionResult = qualityChecker.runDetection(window, this.metricUrn);
      anomalies.addAll(detectionResult.getAnomalies());
    }

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setDetectionConfigId(this.config.getId());
      anomaly.setMetricUrn(this.metricUrn);
      anomaly.setMetric(this.metric.getName());
      anomaly.setCollection(this.metric.getDataset());
      anomaly.setDimensions(DetectionUtils.toFilterMap(this.metricEntity.getFilters()));
      anomaly.getProperties().put(PROP_DETECTOR_COMPONENT_NAME, this.qualityCheckerKey);
      anomaly.getProperties().put(PROP_SUB_ENTITY_NAME, this.entityName);
    }

    return new DetectionPipelineResult(anomalies);
  }
}