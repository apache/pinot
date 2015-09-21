package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import java.util.List;

public class AnomalyFunctionSpecDAO extends AbstractDAO<AnomalyFunctionSpec> {
  public AnomalyFunctionSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyFunctionSpec findById(Long id) {
    return get(id);
  }

  public Long create(AnomalyFunctionSpec anomalyFunctionSpec) {
    return persist(anomalyFunctionSpec).getId();
  }

  public void delete(Long id) {
    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();
    anomalyFunctionSpec.setId(id);
    currentSession().delete(anomalyFunctionSpec);
  }

  public List<AnomalyFunctionSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAll"));
  }

  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAllByCollection").setParameter("collection", collection));
  }
}
