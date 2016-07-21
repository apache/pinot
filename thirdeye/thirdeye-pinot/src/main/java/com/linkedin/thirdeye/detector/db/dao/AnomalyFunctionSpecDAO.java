package com.linkedin.thirdeye.detector.db.dao;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;

public class AnomalyFunctionSpecDAO extends AbstractBaseDAO<AnomalyFunctionSpec> {
  public AnomalyFunctionSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public void toggleActive(Long id, boolean isActive) {
    namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionSpec#toggleActive").setParameter("id", id)
        .setParameter("isActive", isActive).executeUpdate();
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
