package com.linkedin.thirdeye.anomaly;

import java.util.Date;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;


public class ThirdEyeJobContext {

  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeJobContext.class);


  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private SessionFactory sessionFactory;

  private Long anomalyFunctionId;
  private String windowStartIso;
  private String windowEndIso;
  private Date scheduledFireTime;
  private String jobName;

  public ThirdEyeJobContext() {

  }

  public AnomalyJobSpecDAO getAnomalyJobSpecDAO() {
    return anomalyJobSpecDAO;
  }

  public void setAnomalyJobSpecDAO(AnomalyJobSpecDAO anomalyJobSpecDAO) {
    this.anomalyJobSpecDAO = anomalyJobSpecDAO;
  }

  public AnomalyTaskSpecDAO getAnomalyTaskSpecDAO() {
    return anomalyTaskSpecDAO;
  }

  public void setAnomalyTaskSpecDAO(AnomalyTaskSpecDAO anomalyTaskSpecDAO) {
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
  }

  public AnomalyFunctionSpecDAO getAnomalyFunctionSpecDAO() {
    return anomalyFunctionSpecDAO;
  }

  public void setAnomalyFunctionSpecDAO(AnomalyFunctionSpecDAO anomalyFunctionSpecDAO) {
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public String getWindowStartIso() {
    return windowStartIso;
  }

  public void setWindowStartIso(String windowStartIso) {
    this.windowStartIso = windowStartIso;
  }

  public String getWindowEndIso() {
    return windowEndIso;
  }

  public void setWindowEndIso(String windowEndIso) {
    this.windowEndIso = windowEndIso;
  }

  public Date getScheduledFireTime() {
    return scheduledFireTime;
  }

  public void setScheduledFireTime(Date scheduledFireTime) {
    this.scheduledFireTime = scheduledFireTime;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }




}
