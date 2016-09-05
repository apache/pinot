package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

import java.util.List;

public class EmailConfigurationManager extends AbstractManager<EmailConfigurationDTO> {
  private static final String FIND_BY_FUNCTION_ID =
      "select ec from EmailConfiguration ec JOIN ec.functions fn where fn.id=:id";

  public EmailConfigurationManager() {
    super(EmailConfigurationDTO.class);
  }

  @Transactional
  public List<EmailConfigurationDTO> findByFunctionId(Long id) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
        .setParameter("id", id)
        .getResultList();
  }
}
