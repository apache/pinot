package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskDriverConfiguration;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardConfiguration;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import java.util.Collections;
import java.util.List;


public class ThirdEyeAnomalyConfiguration extends ThirdEyeConfiguration {

  private boolean alert = false;
  private boolean autoload = false;
  private boolean classifier = false;
  private boolean dataCompleteness = false;
  private boolean detectionOnboard = false;
  private boolean holidayEventsLoader = false;
  private boolean monitor = false;
  private boolean pinotProxy = false;
  private boolean scheduler = false;
  private boolean worker = false;

  private long id;
  private long holidayRange;
  private String dashboardHost;
  private SmtpConfiguration smtpConfiguration;
  private MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
  private AutoOnboardConfiguration autoOnboardConfiguration = new AutoOnboardConfiguration();
  private TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
  private String failureFromAddress;
  private String failureToAddress;
  private String keyPath;
  private List<String> calendars = Collections.emptyList();

  public long getHolidayRange() {
    return holidayRange;
  }

  public void setHolidayRange(long holidayRange) {
    this.holidayRange = holidayRange;
  }

  public String getKeyPath() {
    return keyPath;
  }

  public void setKeyPath(String keyPath) {
    this.keyPath = keyPath;
  }

  public List<String> getCalendars() {
    return calendars;
  }

  public void setCalendars(List<String> calendars) {
    this.calendars = calendars;
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

  public SmtpConfiguration getSmtpConfiguration() {
    return smtpConfiguration;
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

  public boolean isDetectionOnboard() {
    return detectionOnboard;
  }

  public void setDetectionOnboard(boolean detectionOnboard) {
    this.detectionOnboard = detectionOnboard;
  }

  public void setSmtpConfiguration(SmtpConfiguration smtpConfiguration) {
    this.smtpConfiguration = smtpConfiguration;
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
