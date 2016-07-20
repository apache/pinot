package com.linkedin.thirdeye.detector.db;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;

import io.dropwizard.hibernate.AbstractDAO;

public class AnomalyFunctionSpecDAO extends AbstractDAO<AnomalyFunctionSpec> {
  public AnomalyFunctionSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyFunctionSpec findById(Long id) {
    return get(id);
  }

  public Long createOrUpdate(AnomalyFunctionSpec anomalyFunctionSpec) {
    long id = persist(anomalyFunctionSpec).getId();
    currentSession().getTransaction().commit();
    return id;
  }


  public void toggleActive(Long id, boolean isActive) {
    namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#toggleActive").setParameter("id", id)
        .setParameter("isActive", isActive).executeUpdate();
  }

  public void delete(Long id) {
    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();
    anomalyFunctionSpec.setId(id);
    currentSession().delete(anomalyFunctionSpec);
  }

  public void delete(AnomalyFunctionSpec anomalyFunctionSpec) {
    currentSession().delete(anomalyFunctionSpec);
  }

  public List<AnomalyFunctionSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAll"));
  }

  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAllByCollection")
        .setParameter("collection", collection));
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    List<String> metrics = new ArrayList<>();
    // although query returns list of strings, JPA returns List<AnomalyFunctionSpec> for list(Query) call
    List<AnomalyFunctionSpec> distinctMetricsList = list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#findDistinctMetricsByCollection")
        .setParameter("collection", collection));
    for (Object metric : distinctMetricsList) {
      metrics.add(metric.toString());
    }
    return metrics;
  }

}
