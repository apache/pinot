package com.linkedin.thirdeye.anomaly.task;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private AnomalyJobDAO anomalyJobDAO;
  private AnomalyTaskDAO anomalyTaskDAO;
  private AnomalyResultDAO resultDAO;
  private AnomalyFunctionRelationDAO relationDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;


  public AnomalyJobDAO getAnomalyJobDAO() {
    return anomalyJobDAO;
  }
  public void setAnomalyJobDAO(AnomalyJobDAO anomalyJobDAO) {
    this.anomalyJobDAO = anomalyJobDAO;
  }
  public AnomalyTaskDAO getAnomalyTaskDAO() {
    return anomalyTaskDAO;
  }
  public void setAnomalyTaskDAO(AnomalyTaskDAO anomalyTaskDAO) {
    this.anomalyTaskDAO = anomalyTaskDAO;
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
  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }
  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }
  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }
  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }


}
