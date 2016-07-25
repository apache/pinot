package com.linkedin.thirdeye.detector.db.dao;

import java.util.List;
import org.hibernate.SessionFactory;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

@Deprecated
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

}
