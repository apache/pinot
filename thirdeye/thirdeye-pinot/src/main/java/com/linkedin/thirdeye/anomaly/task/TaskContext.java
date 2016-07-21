package com.linkedin.thirdeye.anomaly.task;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyResultDAO resultDAO;
  private AnomalyFunctionRelationDAO relationDAO;
  private SessionFactory sessionFactory;
  private AnomalyFunctionFactory anomalyFunctionFactory;


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
  public AnomalyResultDAO getResultDAO() {
    return resultDAO;
  }
  public void setResultDAO(AnomalyResultDAO resultDAO) {
    this.resultDAO = resultDAO;
  }
  public AnomalyFunctionRelationDAO getRelationDAO() {
    return relationDAO;
  }
  public void setRelationDAO(AnomalyFunctionRelationDAO relationDAO) {
    this.relationDAO = relationDAO;
  }
  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }
  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }
  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }


}
