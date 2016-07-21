package com.linkedin.thirdeye.anomaly.task;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private AnomalyResultDAO resultDAO;
  private AnomalyFunctionRelationDAO relationDAO;
  private SessionFactory sessionFactory;
  private AnomalyFunctionFactory anomalyFunctionFactory;

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
