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

package org.apache.pinot.thirdeye.anomaly;

import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DataAvailabilitySchedulingConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardConfiguration;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import java.util.List;
import org.apache.pinot.thirdeye.common.restclient.ThirdEyeRestClientConfiguration;


public class ThirdEyeAnomalyConfiguration extends ThirdEyeConfiguration {

  private boolean alert = false;
  private boolean autoload = false;
  private boolean holidayEventsLoader = false;
  private boolean mockEventsLoader = false;
  private boolean monitor = false;
  private boolean pinotProxy = false;
  private boolean scheduler = false;
  private boolean worker = false;
  private boolean onlineWorker = false;
  private boolean detectionPipeline = false;
  private boolean detectionAlert = false;
  private boolean dataAvailabilityEventListener = false;
  private boolean dataAvailabilityTaskScheduler = false;

  private long id;
  private HolidayEventsLoaderConfiguration holidayEventsLoaderConfiguration = new HolidayEventsLoaderConfiguration();
  private MockEventsLoaderConfiguration mockEventsLoaderConfiguration = new MockEventsLoaderConfiguration();
  private MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
  private AutoOnboardConfiguration autoOnboardConfiguration = new AutoOnboardConfiguration();
  private TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
  private ThirdEyeRestClientConfiguration teRestConfig = new ThirdEyeRestClientConfiguration();
  private DataAvailabilitySchedulingConfiguration
      dataAvailabilitySchedulingConfiguration = new DataAvailabilitySchedulingConfiguration();
  private List<String> holidayCountriesWhitelist;

  public ThirdEyeRestClientConfiguration getThirdEyeRestClientConfiguration() {
    return teRestConfig;
  }

  public void setThirdEyeRestClientConfiguration(ThirdEyeRestClientConfiguration teRestConfig) {
    this.teRestConfig = teRestConfig;
  }

  public HolidayEventsLoaderConfiguration getHolidayEventsLoaderConfiguration() {
    return holidayEventsLoaderConfiguration;
  }

  public void setHolidayEventsLoaderConfiguration(HolidayEventsLoaderConfiguration holidayEventsLoaderConfiguration) {
    this.holidayEventsLoaderConfiguration = holidayEventsLoaderConfiguration;
  }

  public boolean isMockEventsLoader() {
    return mockEventsLoader;
  }

  public void setMockEventsLoader(boolean mockEventsLoader) {
    this.mockEventsLoader = mockEventsLoader;
  }

  public MockEventsLoaderConfiguration getMockEventsLoaderConfiguration() {
    return mockEventsLoaderConfiguration;
  }

  public void setMockEventsLoaderConfiguration(MockEventsLoaderConfiguration mockEventsLoaderConfiguration) {
    this.mockEventsLoaderConfiguration = mockEventsLoaderConfiguration;
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

  public boolean isOnlineWorker() {
    return onlineWorker;
  }

  public void setOnlineWorker(boolean onlineWorker) {
    this.onlineWorker = onlineWorker;
  }

  public boolean isMonitor() {
    return monitor;
  }

  public void setMonitor(boolean monitor) {
    this.monitor = monitor;
  }

  public boolean isDataAvailabilityEventListener() {
    return dataAvailabilityEventListener;
  }

  public void setDataAvailabilityEventListener(boolean dataAvailabilityEventListener) {
    this.dataAvailabilityEventListener = dataAvailabilityEventListener;
  }

  public boolean isDataAvailabilityTaskScheduler() {
    return dataAvailabilityTaskScheduler;
  }

  public void setDataAvailabilityEventScheduler(boolean dataAvailabilityEventScheduler) {
    this.dataAvailabilityTaskScheduler = dataAvailabilityEventScheduler;
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

  public boolean isPinotProxy() {
    return pinotProxy;
  }

  public void setPinotProxy(boolean pinotProxy) {
    this.pinotProxy = pinotProxy;
  }

  public List<String> getHolidayCountriesWhitelist() {
    return holidayCountriesWhitelist;
  }

  public void setHolidayCountriesWhitelist(List<String> holidayCountriesWhitelist) {
    this.holidayCountriesWhitelist = holidayCountriesWhitelist;
  }

  public DataAvailabilitySchedulingConfiguration getDataAvailabilitySchedulingConfiguration() {
    return dataAvailabilitySchedulingConfiguration;
  }

  public void setDataAvailabilitySchedulingConfiguration(
      DataAvailabilitySchedulingConfiguration dataAvailabilitySchedulingConfiguration) {
    this.dataAvailabilitySchedulingConfiguration = dataAvailabilitySchedulingConfiguration;
  }
}
