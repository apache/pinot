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

package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskDriverConfiguration;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardConfiguration;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import java.util.Collection;
import java.util.HashSet;


public class ThirdEyeAnomalyConfiguration extends ThirdEyeConfiguration {

  private boolean alert = false;
  private boolean autoload = false;
  private boolean classifier = false;
  private boolean dataCompleteness = false;
  private boolean holidayEventsLoader = false;
  private boolean monitor = false;
  private boolean pinotProxy = false;
  private boolean scheduler = false;
  private boolean worker = false;
  private boolean detectionPipeline = false;
  private boolean detectionAlert = false;

  private long id;
  private String dashboardHost;
  private HolidayEventsLoaderConfiguration holidayEventsLoaderConfiguration = new HolidayEventsLoaderConfiguration();
  private MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
  private AutoOnboardConfiguration autoOnboardConfiguration = new AutoOnboardConfiguration();
  private TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
  private String failureFromAddress;
  private String failureToAddress;

  public HolidayEventsLoaderConfiguration getHolidayEventsLoaderConfiguration() {
    return holidayEventsLoaderConfiguration;
  }

  public void setHolidayEventsLoaderConfiguration(HolidayEventsLoaderConfiguration holidayEventsLoaderConfiguration) {
    this.holidayEventsLoaderConfiguration = holidayEventsLoaderConfiguration;
  }

  public boolean isDetectionAlert() {
    return detectionAlert;
  }

  public void setDetectionAlert(boolean detectionAlert) {
    this.detectionAlert = detectionAlert;
  }

  public boolean isDetectionPipeline() {
    return detectionPipeline;
  }

  public void setDetectionPipeline(boolean detectionPipeline) {
    this.detectionPipeline = detectionPipeline;
  }

  public boolean isHolidayEventsLoader() {
    return holidayEventsLoader;
  }

  public void setHolidayEventsLoader(boolean holidayEventsLoader) {
    this.holidayEventsLoader = holidayEventsLoader;
  }

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public boolean isScheduler() {
    return scheduler;
  }

  public void setScheduler(boolean scheduler) {
    this.scheduler = scheduler;
  }

  public boolean isWorker() {
    return worker;
  }

  public void setWorker(boolean worker) {
    this.worker = worker;
  }

  public boolean isMonitor() {
    return monitor;
  }

  public void setMonitor(boolean monitor) {
    this.monitor = monitor;
  }

  public MonitorConfiguration getMonitorConfiguration() {
    return monitorConfiguration;
  }

  public void setMonitorConfiguration(MonitorConfiguration monitorConfiguration) {
    this.monitorConfiguration = monitorConfiguration;
  }

  public AutoOnboardConfiguration getAutoOnboardConfiguration() {
    return autoOnboardConfiguration;
  }

  public void setAutoOnboardConfiguration(AutoOnboardConfiguration autoOnboardConfiguration) {
    this.autoOnboardConfiguration = autoOnboardConfiguration;
  }

  public TaskDriverConfiguration getTaskDriverConfiguration() {
    return taskDriverConfiguration;
  }

  public void setTaskDriverConfiguration(TaskDriverConfiguration taskDriverConfiguration) {
    this.taskDriverConfiguration = taskDriverConfiguration;
  }

  public boolean isAlert() {
    return alert;
  }

  public void setAlert(boolean alert) {
    this.alert = alert;
  }

  public boolean isAutoload() {
    return autoload;
  }

  public void setAutoload(boolean autoload) {
    this.autoload = autoload;
  }

  public boolean isDataCompleteness() {
    return dataCompleteness;
  }

  public void setDataCompleteness(boolean dataCompleteness) {
    this.dataCompleteness = dataCompleteness;
  }

  public boolean isClassifier() {
    return classifier;
  }

  public void setClassifier(boolean classifier) {
    this.classifier = classifier;
  }

  public boolean isPinotProxy() {
    return pinotProxy;
  }

  public void setPinotProxy(boolean pinotProxy) {
    this.pinotProxy = pinotProxy;
  }

  public String getFailureFromAddress() {
    return failureFromAddress;
  }

  public void setFailureFromAddress(String failureFromAddress) {
    this.failureFromAddress = failureFromAddress;
  }

  public String getFailureToAddress() {
    return failureToAddress;
  }

  public void setFailureToAddress(String failureToAddress) {
    this.failureToAddress = failureToAddress;
  }
}
