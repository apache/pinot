package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskDriverConfiguration;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardConfiguration;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeAnomalyConfiguration extends ThirdEyeConfiguration {
  private boolean scheduler = false;
  private boolean worker = false;
  private boolean monitor = false;
  private boolean alert = false;
  @Deprecated
  private boolean merger = false;
  private boolean autoload = false;
  private boolean dataCompleteness = false;
  private boolean classifier = false;
  private boolean pinotProxy = false;
  private boolean detectionOnboard = false;

  private long id;
  private String dashboardHost;
  private SmtpConfiguration smtpConfiguration;
  private MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
  private AutoOnboardConfiguration autoOnboardConfiguration = new AutoOnboardConfiguration();
  private TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
  private String failureFromAddress;
  private String failureToAddress;

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

  public boolean isMerger() {
    return merger;
  }

  public void setMerger(boolean merger) {
    this.merger = merger;
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
