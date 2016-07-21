package com.linkedin.thirdeye.detector.db;

import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.entity.EmailFunctionDependency;

import io.dropwizard.hibernate.AbstractDAO;

// TODO: EmailFunctionDependency is a mapping, should be handled through parent entity

/**
 * DAO for handling dependencies between email reports and functions.
 */
public class EmailFunctionDependencyDAO extends AbstractDAO<EmailFunctionDependency> {
  /*
   * Note: You can also use the corresponding entity DAOs to update the entity objects themselves
   * and use the persist method to save the changes, eg EmailConfigurationDAO.addFunctionDependency
   */

  public EmailFunctionDependencyDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public void create(EmailFunctionDependency emailFunctionDependency) {
    persist(emailFunctionDependency);
  }

  public void deleteByEmail(Long emailId) {
    namedQuery("com.linkedin.thirdeye.api.EmailFunctionDependency#deleteByEmail")
        .setParameter("emailId", emailId).executeUpdate();
  }

  public void deleteByFunction(Long functionId) {
    namedQuery("com.linkedin.thirdeye.api.EmailFunctionDependency#deleteByFunction")
        .setParameter("functionId", functionId).executeUpdate();
  }

  public void delete(Long emailId, Long functionId) {
    EmailFunctionDependency anomalyFunctionRelation = new EmailFunctionDependency();
    anomalyFunctionRelation.setEmailId(emailId);
    anomalyFunctionRelation.setFunctionId(functionId);
    currentSession().delete(anomalyFunctionRelation);
  }

  public List<EmailFunctionDependency> find() {
    return list(namedQuery("com.linkedin.thirdeye.api.EmailFunctionDependency#find"));
  }

  public List<EmailFunctionDependency> findByEmail(Long emailId) {
    return list(namedQuery("com.linkedin.thirdeye.api.EmailFunctionDependency#findByEmail")
        .setParameter("emailId", emailId));
  }

  public List<EmailFunctionDependency> findByFunction(Long functionId) {
    return list(namedQuery("com.linkedin.thirdeye.api.EmailFunctionDependency#findByFunction")
        .setParameter("functionId", functionId));
  }
}
