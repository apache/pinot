package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Deprecated
@Singleton
public class EmailConfigurationManagerImpl extends AbstractManagerImpl<EmailConfigurationDTO>
    implements EmailConfigurationManager {

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
  public int update(EmailConfigurationDTO emailConfigurationDTO) {
    EmailConfigurationBean emailConfigurationBean =
        (EmailConfigurationBean) convertEmailConfigurationDTO2Bean(emailConfigurationDTO);
    return genericPojoDao.update(emailConfigurationBean);
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
        AnomalyFunctionDTO dto = convertBean2DTO(bean, AnomalyFunctionDTO.class);
        functions.add(dto);
      }
      emailConfigurationDTO.setFunctions(functions);
    }
    return emailConfigurationDTO;
  }

  @Override
  public List<EmailConfigurationDTO> findByFunctionId(Long functionId) {
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

  @Override
  public List<EmailConfigurationDTO> findByCollectionMetric(String collection, String metric) {
    Predicate predicate =
        Predicate.AND(Predicate.EQ("collection", collection), Predicate.EQ("metric", metric));
    List<EmailConfigurationBean> list = genericPojoDao.get(predicate, EmailConfigurationBean.class);
    List<EmailConfigurationDTO> result = new ArrayList<>();
    for (EmailConfigurationBean bean : list) {
      EmailConfigurationDTO dto = convertEmailConfigurationBean2DTO(bean);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<EmailConfigurationDTO> findByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<EmailConfigurationBean> list = genericPojoDao.get(predicate, EmailConfigurationBean.class);
    List<EmailConfigurationDTO> result = new ArrayList<>();
    for (EmailConfigurationBean bean : list) {
      EmailConfigurationDTO dto = convertEmailConfigurationBean2DTO(bean);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<EmailConfigurationDTO> findAll() {
    List<EmailConfigurationDTO> beanList = super.findAll();
    List<EmailConfigurationDTO> result = new ArrayList<>();
    for (EmailConfigurationBean bean : beanList) {
      EmailConfigurationDTO dto = convertEmailConfigurationBean2DTO(bean);
      result.add(dto);
    }
    return result;
  }
}
