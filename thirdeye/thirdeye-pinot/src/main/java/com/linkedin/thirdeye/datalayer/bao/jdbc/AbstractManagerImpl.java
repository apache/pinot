package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dao.GenericPojoDao;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractManagerImpl.class);

  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();

  static {
    MODEL_MAPPER.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
  }

  private Class<? extends AbstractDTO> dtoClass;
  private Class<? extends AbstractBean> beanClass;
  protected GenericPojoDao genericPojoDao;

  protected AbstractManagerImpl(Class<? extends AbstractDTO> dtoClass,
      Class<? extends AbstractBean> beanClass) {
    this.dtoClass = dtoClass;
    this.beanClass = beanClass;
  }

  public void setGenericPojoDao(GenericPojoDao genericPojoDao) {
    this.genericPojoDao = genericPojoDao;
  }

  @Override
  public Long save(E entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;
  }

  @Override
  public int update(E entity, Predicate predicate) {
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    return genericPojoDao.update(bean, predicate);
  }

  @Override
  public int update(E entity) {
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    return genericPojoDao.update(bean);
  }

  public E findById(Long id) {
    AbstractBean abstractBean = genericPojoDao.get(id, beanClass);
    if (abstractBean != null) {
      AbstractDTO abstractDTO = MODEL_MAPPER.map(abstractBean, dtoClass);
      return (E) abstractDTO;
    } else {
      return null;
    }
  }

  @Override
  public void delete(E entity) {
    genericPojoDao.delete(entity.getId(), beanClass);
  }

  @Override
  public void deleteById(Long id) {
    genericPojoDao.delete(id, beanClass);
  }

  @Override
  public List<E> findAll() {
    List<? extends AbstractBean> list = genericPojoDao.getAll(beanClass);
    List<E> result = new ArrayList<>();
    for (AbstractBean bean : list) {
      AbstractDTO dto = MODEL_MAPPER.map(bean, dtoClass);
      result.add((E) dto);
    }
    return result;
  }

  @Override
  public List<E> findByParams(Map<String, Object> filters) {
    List<? extends AbstractBean> list = genericPojoDao.get(filters, beanClass);
    return convertBeanListToDTOList(list);
  }

  @Override
  public List<E> findByPredicate(Predicate predicate) {
    List<? extends AbstractBean> list = genericPojoDao.get(predicate, beanClass);
    return convertBeanListToDTOList(list);
  }

  protected List<E> convertBeanListToDTOList(List<? extends AbstractBean> beans) {
    List<E> result = new ArrayList<>();
    for (AbstractBean bean : beans) {
      result.add((E) convertBean2DTO(bean, dtoClass));
    }
    return result;
  }

  @Deprecated
  protected RawAnomalyResultDTO createRawAnomalyDTOFromBean(
      RawAnomalyResultBean rawAnomalyResultBean) {
    RawAnomalyResultDTO rawAnomalyResultDTO;
    rawAnomalyResultDTO = MODEL_MAPPER.map(rawAnomalyResultBean, RawAnomalyResultDTO.class);
    if (rawAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(rawAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      if (anomalyFunctionBean == null) {
        LOG.error("this anomaly function bean should not be null");
      }
      AnomalyFunctionDTO anomalyFunctionDTO =
          MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      rawAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (rawAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao
          .get(rawAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO =
          MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      rawAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    return rawAnomalyResultDTO;
  }

  protected <T extends AbstractDTO> T convertBean2DTO(AbstractBean entity, Class<T> dtoClass) {
    try {
      AbstractDTO dto = dtoClass.newInstance();
      MODEL_MAPPER.map(entity, dto);
      return (T) dto;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected <T extends AbstractBean> T convertDTO2Bean(AbstractDTO entity, Class<T> beanClass) {
    try {
      AbstractBean bean = beanClass.newInstance();
      MODEL_MAPPER.map(entity, bean);
      return (T) bean;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
