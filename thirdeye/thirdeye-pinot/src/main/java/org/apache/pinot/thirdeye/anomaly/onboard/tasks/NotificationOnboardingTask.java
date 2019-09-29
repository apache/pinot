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

package org.apache.pinot.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.apache.pinot.thirdeye.notification.formatter.channels.EmailContentFormatter;
import org.apache.pinot.thirdeye.notification.content.templates.OnboardingNotificationContent;
import org.apache.pinot.thirdeye.anomaly.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.alert.util.AlertFilterHelper;
import org.apache.pinot.thirdeye.anomaly.alert.util.EmailHelper;
import org.apache.pinot.thirdeye.anomaly.onboard.framework.BaseDetectionOnboardTask;
import org.apache.pinot.thirdeye.anomaly.onboard.framework.DetectionOnboardExecutionContext;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.mail.EmailException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Send out email notifications
 */
public class NotificationOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationOnboardingTask.class);

  public static final String TASK_NAME = "Notification";

  public static final String ALERT_CONFIG = DefaultDetectionOnboardJob.ALERT_CONFIG;
  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String ANOMALY_FUNCTION_CONFIG = DefaultDetectionOnboardJob.ANOMALY_FUNCTION_CONFIG;
  public static final String NOTIFICATION_START = DefaultDetectionOnboardJob.START;
  public static final String NOTIFICATION_END = DefaultDetectionOnboardJob.END;
  public static final String SMTP_HOST = DefaultDetectionOnboardJob.SMTP_HOST;
  public static final String SMTP_PORT = DefaultDetectionOnboardJob.SMTP_PORT;
  public static final String THIRDEYE_DASHBOARD_HOST = DefaultDetectionOnboardJob.THIRDEYE_DASHBOARD_HOST;
  public static final String DEFAULT_ALERT_SENDER_ADDRESS = DefaultDetectionOnboardJob.DEFAULT_ALERT_SENDER_ADDRESS;
  public static final String DEFAULT_ALERT_RECEIVER_ADDRESS = DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS;
  public static final String PHANTON_JS_PATH = DefaultDetectionOnboardJob.PHANTON_JS_PATH;
  public static final String ROOT_DIR = DefaultDetectionOnboardJob.ROOT_DIR;

  public NotificationOnboardingTask(){
    super(TASK_NAME);
  }

  /**
   * Executes the task. To fail this task, throw exceptions. The job executor will catch the exception and store
   * it in the message in the execution status of this task.
   */
  @Override
  public void run() {
    Configuration taskConfigs = taskContext.getConfiguration();
    DetectionOnboardExecutionContext executionContext = taskContext.getExecutionContext();

    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_FACTORY));

    AlertFilterFactory alertFilterFactory = (AlertFilterFactory) executionContext.getExecutionResult(ALERT_FILTER_FACTORY);

    Preconditions.checkNotNull(alertFilterFactory);

    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_CONFIG));
    Preconditions.checkNotNull(executionContext.getExecutionResult(ANOMALY_FUNCTION_CONFIG));
    Preconditions.checkNotNull(executionContext.getExecutionResult(NOTIFICATION_START));
    Preconditions.checkNotNull(executionContext.getExecutionResult(NOTIFICATION_END));
    Preconditions.checkNotNull(taskConfigs.getString(SMTP_HOST));
    Preconditions.checkNotNull(taskConfigs.getString(SMTP_PORT));
    Preconditions.checkNotNull(taskConfigs.getString(DEFAULT_ALERT_RECEIVER_ADDRESS));
    Preconditions.checkNotNull(taskConfigs.getString(DEFAULT_ALERT_SENDER_ADDRESS));
    Preconditions.checkNotNull(taskConfigs.getString(THIRDEYE_DASHBOARD_HOST));
    Preconditions.checkNotNull(taskConfigs.getString(PHANTON_JS_PATH));
    Preconditions.checkNotNull(taskConfigs.getString(ROOT_DIR));

    AnomalyFunctionDTO anomalyFunctionSpec = (AnomalyFunctionDTO) executionContext.getExecutionResult(ANOMALY_FUNCTION_CONFIG);
    Long functionId = anomalyFunctionSpec.getId();
    DateTime start = (DateTime) executionContext.getExecutionResult(NOTIFICATION_START);
    DateTime end = (DateTime) executionContext.getExecutionResult(NOTIFICATION_END);

    // Get alert config from previous output
    AlertConfigDTO alertConfig = (AlertConfigDTO) executionContext.getExecutionResult(ALERT_CONFIG);

    SmtpConfiguration smtpConfiguration = new SmtpConfiguration();
    smtpConfiguration.setSmtpHost(taskConfigs.getString(SMTP_HOST));
    smtpConfiguration.setSmtpPort(taskConfigs.getInt(SMTP_PORT));

    MergedAnomalyResultManager mergeAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> anomalyCandidates = mergeAnomalyDAO
        .findByFunctionId(functionId);
    anomalyCandidates = AlertFilterHelper.applyFiltrationRule(anomalyCandidates, alertFilterFactory);
    List<AnomalyResult> filteredAnomalyResults = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalyCandidates) {
      filteredAnomalyResults.add(anomaly);
    }

    // Email Subject
    String subject = String.format("Replay results for %s is ready for review!",
        DAORegistry.getInstance().getAnomalyFunctionDAO().findById(functionId).getFunctionName());

    // construct context
    ADContentFormatterContext context = new ADContentFormatterContext();
    context.setAnomalyFunctionSpec(anomalyFunctionSpec);
    context.setAlertConfig(alertConfig);
    context.setStart(start);
    context.setEnd(end);

    // Set up thirdeye config
    ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeAnomalyConfig.setDashboardHost(taskConfigs.getString(THIRDEYE_DASHBOARD_HOST));
    thirdEyeAnomalyConfig.setPhantomJsPath(taskConfigs.getString(PHANTON_JS_PATH));
    thirdEyeAnomalyConfig.setRootDir(taskConfigs.getString(ROOT_DIR));

    EmailContentFormatter
        emailFormatter = new EmailContentFormatter(new OnboardingNotificationContent(), thirdEyeAnomalyConfig);
    EmailEntity emailEntity = emailFormatter.getEmailEntity(alertConfig.getReceiverAddresses(),
        subject, filteredAnomalyResults, context);
    try {
      EmailHelper.sendEmailWithEmailEntity(emailEntity, smtpConfiguration);
    } catch (EmailException e) {
      LOG.error("Unable to send out email to recipients");
      throw new IllegalStateException("Unable to send out email to recipients", e);
    }

    // Set the alert to be active after everything is successful
    alertConfig.setActive(true);
    DAORegistry.getInstance().getAlertConfigDAO().update(alertConfig);

    // Update Notified flag
    for (MergedAnomalyResultDTO notifiedAnomaly : anomalyCandidates) {
      notifiedAnomaly.setNotified(true);
      mergeAnomalyDAO.update(notifiedAnomaly);
    }
  }
}
