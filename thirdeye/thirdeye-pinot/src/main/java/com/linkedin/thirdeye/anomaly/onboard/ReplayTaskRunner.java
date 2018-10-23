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

package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnBoardJobRunner;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardJobContext;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardJobStatus;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


/**
 * Traditional ThirdEye task runner wrapping an onboarding framework job
 */
public class ReplayTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ReplayTaskRunner.class);

  private final AnomalyFunctionManager anomalyFunctionDAO;

  public ReplayTaskRunner() {
    this.anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ReplayTaskInfo replayTaskInfo = (ReplayTaskInfo)  taskInfo;

    // fetch anomaly function
    final String jobName = replayTaskInfo.getJobName();
    final AnomalyFunctionDTO anomalyFunction = this.anomalyFunctionDAO.findWhereNameEquals(jobName);
    Preconditions.checkNotNull(anomalyFunction, String.format("Could not find anomaly function '%s'", jobName));

    final long jobId = anomalyFunction.getId();

    try {
      // Put System Configuration into properties
      Map<String, String> properties = new HashMap<>(replayTaskInfo.getProperties());
      Configuration systemConfig = toConfiguration(taskContext.getThirdEyeAnomalyConfiguration());
      Iterator systemConfigKeyIterator = systemConfig.getKeys();
      while (systemConfigKeyIterator.hasNext()) {
        String systemConfigKey = systemConfigKeyIterator.next().toString();
        properties.put(systemConfigKey, systemConfig.getString(systemConfigKey));
      }

      LOG.info("Creating replay job with properties: {}", properties);

      DetectionOnboardJob job = new DefaultDetectionOnboardJob(replayTaskInfo.getJobName(), properties);

      Preconditions.checkNotNull(job, "Job cannot be null.");
      Preconditions.checkNotNull(job.getName(), "Job name cannot be null.");

      // Initialize the tasks and their configuration
      Configuration configuration = job.getTaskConfiguration();
      Preconditions.checkNotNull(configuration, String.format("Job %s returns a null configuration.", jobName));

      List<DetectionOnboardTask> tasks = job.getTasks();
      Preconditions.checkNotNull(tasks, "Job %s returns a null task list.", jobName);

      DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");
      DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);
      DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);

      // execute
      jobRunner.run();

      // update job status
      updateJobStatus(jobId, jobStatus);

    } catch (Exception e) {
      LOG.error("Replay job failed", e);
      updateJobStatus(jobId, new DetectionOnboardJobStatus(jobId, jobName,
          JobConstants.JobStatus.FAILED, String.format("Execution Error: %s", ExceptionUtils.getStackTrace(e))));
    }

    return Collections.emptyList();
  }

  private void updateJobStatus(long jobId, DetectionOnboardJobStatus jobStatus) {
    final AnomalyFunctionDTO anomalyFunction = this.anomalyFunctionDAO.findById(jobId);
    anomalyFunction.setOnboardJobStatus(jobStatus);
    this.anomalyFunctionDAO.save(anomalyFunction);
  }

  private static Configuration toConfiguration(ThirdEyeAnomalyConfiguration thirdeyeConfigs) {
    Preconditions.checkNotNull(thirdeyeConfigs);
    SmtpConfiguration smtpConfiguration = SmtpConfiguration.createFromProperties(
        thirdeyeConfigs.getAlerterConfiguration().get(SMTP_CONFIG_KEY));
    Preconditions.checkNotNull(smtpConfiguration);

    Map<String, String> systemConfig = new HashMap<>();
    systemConfig.put(DefaultDetectionOnboardJob.FUNCTION_FACTORY_CONFIG_PATH, thirdeyeConfigs.getFunctionConfigPath());
    systemConfig.put(DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY_CONFIG_PATH, thirdeyeConfigs.getAlertFilterConfigPath());
    systemConfig.put(DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH, thirdeyeConfigs.getFilterAutotuneConfigPath());
    systemConfig.put(DefaultDetectionOnboardJob.SMTP_HOST, smtpConfiguration.getSmtpHost());
    systemConfig.put(DefaultDetectionOnboardJob.SMTP_PORT, Integer.toString(smtpConfiguration.getSmtpPort()));
    systemConfig.put(DefaultDetectionOnboardJob.THIRDEYE_DASHBOARD_HOST, thirdeyeConfigs.getDashboardHost());
    systemConfig.put(DefaultDetectionOnboardJob.PHANTON_JS_PATH, thirdeyeConfigs.getPhantomJsPath());
    systemConfig.put(DefaultDetectionOnboardJob.ROOT_DIR, thirdeyeConfigs.getRootDir());
    systemConfig.put(DefaultDetectionOnboardJob.DEFAULT_ALERT_SENDER_ADDRESS, thirdeyeConfigs.getFailureFromAddress());
    systemConfig.put(DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS, thirdeyeConfigs.getFailureToAddress());

    return new MapConfiguration(systemConfig);
  }

}
