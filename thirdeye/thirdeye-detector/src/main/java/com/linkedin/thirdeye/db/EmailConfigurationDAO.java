package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.api.EmailConfiguration;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import java.util.List;

public class EmailConfigurationDAO extends AbstractDAO<EmailConfiguration> {
  public EmailConfigurationDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public EmailConfiguration findById(Long id) {
    return get(id);
  }

  public Long create(EmailConfiguration anomalyResult) {
    return persist(anomalyResult).getId();
  }

  public void delete(Long id) {
    EmailConfiguration anomalyResult = new EmailConfiguration();
    anomalyResult.setId(id);
    currentSession().delete(id);
  }

  public List<EmailConfiguration> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.api.EmailConfiguration#findAll"));
  }
}
