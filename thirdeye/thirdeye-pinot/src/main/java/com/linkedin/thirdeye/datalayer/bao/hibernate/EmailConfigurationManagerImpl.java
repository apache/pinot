package com.linkedin.thirdeye.datalayer.bao.hibernate;

import com.google.common.collect.ImmutableMap;
import java.util.List;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

public class EmailConfigurationManagerImpl extends AbstractManagerImpl<EmailConfigurationDTO> implements EmailConfigurationManager {
  private static final String FIND_BY_FUNCTION_ID =
      "select ec from EmailConfigurationDTO ec JOIN ec.functions fn where fn.id=:id";

  public EmailConfigurationManagerImpl() {
    super(EmailConfigurationDTO.class);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IEmailConfigurationManager#findByFunctionId(java.lang.Long)
   */
  @Override
  @Transactional
  public List<EmailConfigurationDTO> findByFunctionId(Long id) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
        .setParameter("id", id)
        .getResultList();
  }

  @Override
  @Transactional
  public List<EmailConfigurationDTO> findByCollectionMetric(String collection,
      String metric) {
    return findByParams(ImmutableMap.of("collection", collection, "metric", metric));
  }

  @Override
  public List<EmailConfigurationDTO> findByCollection(String collection) {
    return findByParams(ImmutableMap.of("collection", collection));
  }
}
