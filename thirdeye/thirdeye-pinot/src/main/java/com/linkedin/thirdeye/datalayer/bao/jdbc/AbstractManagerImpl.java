package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.modelmapper.spi.MatchingStrategy;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dao.GenericPojoDao;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public abstract class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {

  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();

  static {
    MODEL_MAPPER.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
  }

  protected static GenericPojoDao genericPojoDao =
      DaoProviderUtil.getInstance(GenericPojoDao.class);

  Class<? extends AbstractDTO> dtoClass;
  Class<? extends AbstractBean> beanClass;

  protected AbstractManagerImpl(Class<? extends AbstractDTO> dtoClass,
      Class<? extends AbstractBean> beanClass) {
    this.dtoClass = dtoClass;
    this.beanClass = beanClass;
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
  public void update(E entity) {
    AbstractBean bean = convertDTO2Bean(entity, beanClass);
    genericPojoDao.update(bean);
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
    List<E> result = new ArrayList<>();
    for (AbstractBean bean : list) {
      AbstractDTO dto = MODEL_MAPPER.map(bean, dtoClass);
      result.add((E) dto);
    }
    return result;
  }

  protected RawAnomalyResultDTO createRawAnomalyDTOFromBean(
      RawAnomalyResultBean rawAnomalyResultBean) {
    RawAnomalyResultDTO rawAnomalyResultDTO;
    rawAnomalyResultDTO = MODEL_MAPPER.map(rawAnomalyResultBean, RawAnomalyResultDTO.class);
    if (rawAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(rawAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
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
    AbstractDTO dto;
    try {
      dto = dtoClass.newInstance();
      MODEL_MAPPER.map(entity, dto);
      return (T) dto;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected <T extends AbstractBean> T convertDTO2Bean(AbstractDTO entity, Class<T> beanClass) {
    AbstractBean bean;
    try {
      bean = beanClass.newInstance();
      MODEL_MAPPER.map(entity, bean);
      return (T) bean;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected MergedAnomalyResultBean convertMergeAnomalyDTO2Bean(MergedAnomalyResultDTO entity) {
    MergedAnomalyResultBean bean =
        (MergedAnomalyResultBean) convertDTO2Bean(entity, MergedAnomalyResultBean.class);
    if (entity.getFeedback() != null) {
      if (entity.getFeedback().getId() == null) {
        AnomalyFeedbackBean feedbackBean =
            (AnomalyFeedbackBean) convertDTO2Bean(entity.getFeedback(), AnomalyFeedbackBean.class);
        Long feedbackId = genericPojoDao.put(feedbackBean);
        entity.getFeedback().setId(feedbackId);
      }
      bean.setAnomalyFeedbackId(entity.getFeedback().getId());
    }
    if (entity.getFunction() != null) {
      bean.setFunctionId(entity.getFunction().getId());
    }

    if (entity.getAnomalyResults() != null && !entity.getAnomalyResults().isEmpty()) {
      List<Long> rawAnomalyIds = new ArrayList<>();
      for (RawAnomalyResultDTO rawAnomalyDTO : entity.getAnomalyResults()) {
        rawAnomalyIds.add(rawAnomalyDTO.getId());
      }
      bean.setRawAnomalyIdList(rawAnomalyIds);
    }
    return bean;
  }

  protected MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(
      MergedAnomalyResultBean mergedAnomalyResultBean) {
    MergedAnomalyResultDTO mergedAnomalyResultDTO;
    mergedAnomalyResultDTO =
        MODEL_MAPPER.map(mergedAnomalyResultBean, MergedAnomalyResultDTO.class);
    if (mergedAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(mergedAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      AnomalyFunctionDTO anomalyFunctionDTO =
          MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      mergedAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (mergedAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao
          .get(mergedAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO =
          MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      mergedAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    if (mergedAnomalyResultBean.getRawAnomalyIdList() != null
        && !mergedAnomalyResultBean.getRawAnomalyIdList().isEmpty()) {
      List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
      List<RawAnomalyResultBean> list = genericPojoDao
          .get(mergedAnomalyResultBean.getRawAnomalyIdList(), RawAnomalyResultBean.class);
      for (RawAnomalyResultBean rawAnomalyResultBean : list) {
        anomalyResults.add(createRawAnomalyDTOFromBean(rawAnomalyResultBean));
      }
      mergedAnomalyResultDTO.setAnomalyResults(anomalyResults);
    }

    return mergedAnomalyResultDTO;
  }
}
