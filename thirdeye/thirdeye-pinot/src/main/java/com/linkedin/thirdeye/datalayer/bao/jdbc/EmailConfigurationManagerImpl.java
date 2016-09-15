package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
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

  @Override
  public Long save(EmailConfigurationDTO emailConfigurationDTO) {
    if (emailConfigurationDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(emailConfigurationDTO);
      return emailConfigurationDTO.getId();
    }
    EmailConfigurationBean bean =
        (EmailConfigurationBean) convertEmailConfigurationDTO2Bean(emailConfigurationDTO);
    Long id = genericPojoDao.put(bean);
    emailConfigurationDTO.setId(id);
    return id;
  }

  private EmailConfigurationBean convertEmailConfigurationDTO2Bean(
      EmailConfigurationDTO emailConfigurationDTO) {
    EmailConfigurationBean emailConfigurationBean =
        convertDTO2Bean(emailConfigurationDTO, EmailConfigurationBean.class);
    List<Long> functionIds = new ArrayList<>();
    for (AnomalyFunctionDTO function : emailConfigurationDTO.getFunctions()) {
      functionIds.add(function.getId());
    }
    emailConfigurationBean.setFunctionIds(functionIds);
    return emailConfigurationBean;
  }

  @Override
  public void update(EmailConfigurationDTO emailConfigurationDTO) {
    EmailConfigurationBean emailConfigurationBean =
        (EmailConfigurationBean) convertEmailConfigurationDTO2Bean(emailConfigurationDTO);
    genericPojoDao.update(emailConfigurationBean);
  }

  @Override
  public EmailConfigurationDTO findById(Long id) {
    EmailConfigurationBean emailConfigurationBean =
        genericPojoDao.get(id, EmailConfigurationBean.class);
    if (emailConfigurationBean != null) {
      EmailConfigurationDTO emailConfigurationDTO =
          convertEmailConfigurationBean2DTO(emailConfigurationBean);
      return emailConfigurationDTO;
    }
    return null;
  }

  private EmailConfigurationDTO convertEmailConfigurationBean2DTO(
      EmailConfigurationBean emailConfigurationBean) {
    EmailConfigurationDTO emailConfigurationDTO =
        convertBean2DTO(emailConfigurationBean, EmailConfigurationDTO.class);
    List<Long> functionIds = emailConfigurationBean.getFunctionIds();
    if (functionIds != null && !functionIds.isEmpty()) {
      List<AnomalyFunctionBean> list = genericPojoDao.get(functionIds, AnomalyFunctionBean.class);
      List<AnomalyFunctionDTO> functions = new ArrayList<>();
      for (AnomalyFunctionBean bean : list) {
        AnomalyFunctionDTO dto =
            (AnomalyFunctionDTO) convertBean2DTO(bean, AnomalyFunctionDTO.class);
        functions.add(dto);
      }
      emailConfigurationDTO.setFunctions(functions);
    }
    return emailConfigurationDTO;
  }

  @Override
  public List<EmailConfigurationDTO> findByFunctionId(Long functionId) {
    //    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
    //        .setParameter("id", id)
    //        .getResultList();
    List<EmailConfigurationBean> list = genericPojoDao.getAll(EmailConfigurationBean.class);
    List<EmailConfigurationDTO> result = new ArrayList<>();
    for (EmailConfigurationBean bean : list) {
      if (bean.getFunctionIds() != null && bean.getFunctionIds().contains(functionId)) {
        EmailConfigurationDTO dto = convertEmailConfigurationBean2DTO(bean);
        result.add(dto);
      }
    }
    return result;
  }
}
