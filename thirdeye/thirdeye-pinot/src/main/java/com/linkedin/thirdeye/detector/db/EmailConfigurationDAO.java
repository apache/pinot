package com.linkedin.thirdeye.detector.db;

import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;

import io.dropwizard.hibernate.AbstractDAO;

public class EmailConfigurationDAO extends AbstractDAO<EmailConfiguration> {
  public EmailConfigurationDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public EmailConfiguration findById(Long id) {
    return get(id);
  }

  public Long create(EmailConfiguration emailConfiguration) {
    return persist(emailConfiguration).getId();
  }

  public void delete(Long id) {
    EmailConfiguration anomalyResult = new EmailConfiguration();
    anomalyResult.setId(id);
    currentSession().delete(anomalyResult);
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
