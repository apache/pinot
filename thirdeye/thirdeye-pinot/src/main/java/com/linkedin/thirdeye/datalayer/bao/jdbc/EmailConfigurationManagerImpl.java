package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class EmailConfigurationManagerImpl extends AbstractManagerImpl<EmailConfigurationDTO>
    implements EmailConfigurationManager {
  private static final String FIND_BY_FUNCTION_ID =
      "select ec from EmailConfigurationDTO ec JOIN ec.functions fn where fn.id=:id";

  public EmailConfigurationManagerImpl() {
    super(EmailConfigurationDTO.class, EmailConfigurationBean.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.linkedin.thirdeye.datalayer.bao.IEmailConfigurationManager#findByFunctionId(java.lang.Long)
   */
  @Override
  @Transactional
  public List<EmailConfigurationDTO> findByFunctionId(Long id) {
    //    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
    //        .setParameter("id", id)
    //        .getResultList();
    Predicate predicate = Predicate.EQ("active", true);
    List<EmailConfigurationBean> list = genericPojoDao.get(predicate, EmailConfigurationBean.class);
    List<EmailConfigurationDTO> result = new ArrayList<>();
    for (AbstractBean abstractBean : list) {
      EmailConfigurationDTO dto = MODEL_MAPPER.map(abstractBean, dtoClass);
      result.add(dto);
    }
    return result;
  }
}
