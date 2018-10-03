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

package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.onboard.framework.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataPreparationOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(DataPreparationOnboardingTask.class);

  public static final String TASK_NAME = "DataPreparation";

  public static final String FUNCTION_FACTORY_CONFIG_PATH = DefaultDetectionOnboardJob.FUNCTION_FACTORY_CONFIG_PATH;
  public static final String ALERT_FILTER_FACTORY_CONFIG_PATH = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY_CONFIG_PATH;
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH = DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH;
  public static final String FUNCTION_FACTORY = DefaultDetectionOnboardJob.FUNCTION_FACTORY;
  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY;

  public DataPreparationOnboardingTask(){
    super(TASK_NAME);
  }

  @Override
  public void run(){
    Preconditions.checkNotNull(getTaskContext());

    DetectionOnboardTaskContext taskContext = getTaskContext();
    DetectionOnboardExecutionContext executionContext = taskContext.getExecutionContext();
    Configuration configuration = taskContext.getConfiguration();

    Preconditions.checkNotNull(executionContext);
    Preconditions.checkNotNull(configuration);
    Preconditions.checkNotNull(configuration.getString(FUNCTION_FACTORY_CONFIG_PATH));
    Preconditions.checkNotNull(configuration.getString(ALERT_FILTER_FACTORY_CONFIG_PATH));
    Preconditions.checkNotNull(configuration.getString(ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH));

    AnomalyFunctionFactory anomalyFunctionFactory =
        new AnomalyFunctionFactory(configuration.getString(FUNCTION_FACTORY_CONFIG_PATH));
    AlertFilterFactory alertFilterFactory =
        new AlertFilterFactory(configuration.getString(ALERT_FILTER_FACTORY_CONFIG_PATH));
    AlertFilterAutotuneFactory alertFilterAutotuneFactory =
        new AlertFilterAutotuneFactory(configuration.getString(ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH));

    Preconditions.checkNotNull(anomalyFunctionFactory);
    Preconditions.checkNotNull(alertFilterFactory);
    Preconditions.checkNotNull(alertFilterAutotuneFactory);

    executionContext.setExecutionResult(FUNCTION_FACTORY, anomalyFunctionFactory);
    executionContext.setExecutionResult(ALERT_FILTER_FACTORY, alertFilterFactory);
    executionContext.setExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY, alertFilterAutotuneFactory);
  }
}
