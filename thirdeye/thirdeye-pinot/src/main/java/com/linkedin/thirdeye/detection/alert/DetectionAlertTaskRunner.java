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

package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.detection.CurrentAndBaselineLoader;
import com.linkedin.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import com.linkedin.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
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
  private CurrentAndBaselineLoader currentAndBaselineLoader;
  private DetectionAlertConfigManager alertConfigDAO;

  public DetectionAlertTaskRunner() {
    this.detAlertTaskFactory = new DetectionAlertTaskFactory();
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();

    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    this.currentAndBaselineLoader = new CurrentAndBaselineLoader(metricDAO, datasetDAO, aggregationLoader);
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

      // Suppress alerts if any and get the filtered anomalies to be notified
      Set<DetectionAlertSuppressor> alertSuppressors = detAlertTaskFactory.loadAlertSuppressors(alertConfig);
      for (DetectionAlertSuppressor alertSuppressor : alertSuppressors) {
        result = alertSuppressor.run(result);
      }

      // TODO: Cleanup currentAndBaselineLoader
      // In the new design, we have decided to move this function back to the detection pipeline.
      this.currentAndBaselineLoader.fillInCurrentAndBaselineValue(result.getAllAnomalies());

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
