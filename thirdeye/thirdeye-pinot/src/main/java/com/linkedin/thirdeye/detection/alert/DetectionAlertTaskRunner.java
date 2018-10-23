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
import com.linkedin.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * The Detection alert task runner. This runner looks for the new anomalies and run the detection alert filter to get
 * mappings from anomalies to recipients and then send email to the recipients.
 */
public class DetectionAlertTaskRunner implements TaskRunner {

  private final DetectionAlertTaskFactory detectionAlertTaskFactory;

  public DetectionAlertTaskRunner() {
    this.detectionAlertTaskFactory = new DetectionAlertTaskFactory();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ThirdeyeMetricsUtil.alertTaskCounter.inc();

    try {
      long alertId = ((DetectionAlertTaskInfo) taskInfo).getDetectionAlertConfigId();

      DetectionAlertFilter alertFilter = detectionAlertTaskFactory.loadAlertFilter(alertId, System.currentTimeMillis());
      DetectionAlertFilterResult result = alertFilter.run();

      Set<DetectionAlertScheme> alertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertId, taskContext, result);
      for (DetectionAlertScheme alertScheme : alertSchemes) {
        alertScheme.run();
      }

      return new ArrayList<>();
    } finally {
      ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
    }
  }
}
