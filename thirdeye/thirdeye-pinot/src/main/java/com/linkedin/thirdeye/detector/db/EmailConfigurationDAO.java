package com.linkedin.thirdeye.detector.db;

import io.dropwizard.hibernate.AbstractDAO;

import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.api.EmailConfiguration;

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
}
