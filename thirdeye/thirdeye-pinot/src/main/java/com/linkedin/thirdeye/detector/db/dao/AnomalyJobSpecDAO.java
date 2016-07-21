package com.linkedin.thirdeye.detector.db.dao;

import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.entity.AnomalyJobSpec;

public class AnomalyJobSpecDAO extends AbstractBaseDAO<AnomalyJobSpec> {
  public AnomalyJobSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public List<AnomalyJobSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#findAll"));
  }
}
