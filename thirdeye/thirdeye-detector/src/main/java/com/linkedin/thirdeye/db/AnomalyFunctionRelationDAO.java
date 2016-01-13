package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.api.AnomalyFunctionRelation;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import java.util.List;

public class AnomalyFunctionRelationDAO extends AbstractDAO<AnomalyFunctionRelation> {
  public AnomalyFunctionRelationDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public void create(AnomalyFunctionRelation anomalyFunctionRelation) {
    persist(anomalyFunctionRelation);
  }

  public void delete(Long parentId) {
    namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionRelation#deleteByParent")
        .setParameter("parentId", parentId).executeUpdate();
  }

  public void delete(Long parentId, Long childId) {
    AnomalyFunctionRelation anomalyFunctionRelation = new AnomalyFunctionRelation();
    anomalyFunctionRelation.setParentId(parentId);
    anomalyFunctionRelation.setChildId(childId);
    currentSession().delete(anomalyFunctionRelation);
  }

  public List<AnomalyFunctionRelation> find() {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionRelation#find"));
  }

  public List<AnomalyFunctionRelation> findByParent(Long parentId) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyFunctionRelation#findByParent")
        .setParameter("parentId", parentId));
  }
}
