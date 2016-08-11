package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.util.List;

public class EmailConfigurationDAO extends AbstractJpaDAO<EmailConfiguration> {
  private static final String FIND_BY_FUNCTION_ID =
      "select ec from EmailConfiguration ec, AnomalyFunctionSpec fn where fn.id=:id "
          + "and fn in elements(ec.functions)";

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
