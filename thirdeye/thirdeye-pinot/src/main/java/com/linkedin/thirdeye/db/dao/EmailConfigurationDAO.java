package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.util.List;

public class EmailConfigurationDAO extends AbstractJpaDAO<EmailConfiguration> {
  private static final String FIND_BY_FUNCTION_ID =
      "select ec from EmailConfiguration ec JOIN ec.functions fn where fn.id=:id";

  public EmailConfigurationDAO() {
    super(EmailConfiguration.class);
  }

  @Transactional
  public List<EmailConfiguration> findByFunctionId(Long id) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
        .setParameter("id", id)
        .getResultList();
  }
}
