package com.linkedin.thirdeye.anomaly.job;


import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;


public class JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContext.class);

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;

  private Long anomalyFunctionId;
  private String windowStartIso;
  private String windowEndIso;
  private DateTime windowStart;
  private DateTime windowEnd;
  private String jobName;

  private long jobExecutionId;

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
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


  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public DateTime getWindowStart() {
    return windowStart;
  }

  public void setWindowStart(DateTime windowStart) {
    this.windowStart = windowStart;
  }

  public DateTime getWindowEnd() {
    return windowEnd;
  }

  public void setWindowEnd(DateTime windowEnd) {
    this.windowEnd = windowEnd;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }


}
