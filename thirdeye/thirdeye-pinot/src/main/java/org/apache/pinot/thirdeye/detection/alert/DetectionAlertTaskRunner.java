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

package org.apache.pinot.thirdeye.detection.alert;

import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.detection.CurrentAndBaselineLoader;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import org.apache.pinot.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert task runner. This runner looks for the new anomalies and run the detection alert filter to get
 * mappings from anomalies to recipients and then send email to the recipients.
 */
public class DetectionAlertTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskRunner.class);

  private final DetectionAlertTaskFactory detAlertTaskFactory;
  private DetectionAlertConfigManager alertConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyDAO;

  public DetectionAlertTaskRunner() {
    this.detAlertTaskFactory = new DetectionAlertTaskFactory();
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
  }

  private DetectionAlertConfigDTO loadDetectionAlertConfig(long detectionAlertConfigId) {
    DetectionAlertConfigDTO detectionAlertConfig = this.alertConfigDAO.findById(detectionAlertConfigId);
    if (detectionAlertConfig == null) {
      throw new RuntimeException("Cannot find detection alert config id " + detectionAlertConfigId);
    }

    if (detectionAlertConfig.getProperties() == null) {
      LOG.warn(String.format("Detection alert %d contains no properties", detectionAlertConfigId));
    }
    return detectionAlertConfig;
  }

  private void updateAlertConfigWatermarks(DetectionAlertFilterResult result, DetectionAlertConfigDTO alertConfig) {
    long highWaterMark = AlertUtils.getHighWaterMark(result.getAllAnomalies());
    if (alertConfig.getHighWaterMark() != null) {
      highWaterMark = Math.max(alertConfig.getHighWaterMark(), highWaterMark);
    }

    alertConfig.setHighWaterMark(highWaterMark);
    alertConfig.setVectorClocks(
        AlertUtils.mergeVectorClock(alertConfig.getVectorClocks(),
        AlertUtils.makeVectorClock(result.getAllAnomalies()))
    );

    LOG.info("Updating watermarks for alertConfigDAO : {}", alertConfig.getId());
    this.alertConfigDAO.save(alertConfig);
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ThirdeyeMetricsUtil.alertTaskCounter.inc();

    try {
      long alertId = ((DetectionAlertTaskInfo) taskInfo).getDetectionAlertConfigId();
      DetectionAlertConfigDTO alertConfig = loadDetectionAlertConfig(alertId);

      // Load all the anomalies along with their recipients
      DetectionAlertFilter alertFilter = detAlertTaskFactory.loadAlertFilter(alertConfig, System.currentTimeMillis());
      DetectionAlertFilterResult result = alertFilter.run();

      // TODO: The old UI relies on notified tag to display the anomalies. After the migration
      // we need to clean up all references to notified tag.
      for (MergedAnomalyResultDTO anomaly : result.getAllAnomalies()) {
        anomaly.setNotified(true);
        mergedAnomalyDAO.update(anomaly);
      }

      // Suppress alerts if any and get the filtered anomalies to be notified
      Set<DetectionAlertSuppressor> alertSuppressors = detAlertTaskFactory.loadAlertSuppressors(alertConfig);
      for (DetectionAlertSuppressor alertSuppressor : alertSuppressors) {
        result = alertSuppressor.run(result);
      }

      // Send out alert notifications (email and/or iris)
      Set<DetectionAlertScheme> alertSchemes =
          detAlertTaskFactory.loadAlertSchemes(alertConfig, taskContext.getThirdEyeAnomalyConfiguration(), result);
      for (DetectionAlertScheme alertScheme : alertSchemes) {
        alertScheme.run();
      }

      updateAlertConfigWatermarks(result, alertConfig);
      return new ArrayList<>();
    } finally {
      ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
    }
  }
}
