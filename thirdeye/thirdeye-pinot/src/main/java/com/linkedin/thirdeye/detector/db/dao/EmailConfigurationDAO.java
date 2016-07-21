package com.linkedin.thirdeye.detector.db.dao;

import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.entity.EmailConfiguration;

public class EmailConfigurationDAO extends AbstractBaseDAO<EmailConfiguration> {
  public EmailConfigurationDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }


  public void toggleActive(Long id, boolean isActive) {
    namedQuery("com.linkedin.thirdeye.api.EmailConfiguration#toggleActive").setParameter("id", id)
        .setParameter("isActive", isActive).executeUpdate();
  }

  public List<EmailConfiguration> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.api.EmailConfiguration#findAll"));
  }

  /*
   * These methods are provided as an alternative to EmailFunctionDependencyDAO
   */
  public EmailConfiguration addFunctionDependency(EmailConfiguration emailConfiguration,
      AnomalyFunctionSpec functionSpec) {
    List<AnomalyFunctionSpec> dependencies = emailConfiguration.getFunctions();
    if (!dependencies.contains(functionSpec)) {
      dependencies.add(functionSpec);
    }
    return persist(emailConfiguration);
  }

  public EmailConfiguration deleteFunctionDependency(EmailConfiguration emailConfiguration,
      AnomalyFunctionSpec functionSpec) {
    List<AnomalyFunctionSpec> dependencies = emailConfiguration.getFunctions();
    dependencies.remove(functionSpec);
    return persist(emailConfiguration);
  }

}
