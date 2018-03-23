package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO> implements RawAnomalyResultManager {
  private static final Logger LOG = LoggerFactory.getLogger(RawAnomalyResultManagerImpl.class);

  public RawAnomalyResultManagerImpl() {
    super(RawAnomalyResultDTO.class, RawAnomalyResultBean.class);
  }

  public Long save(RawAnomalyResultDTO entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    RawAnomalyResultBean bean =
        (RawAnomalyResultBean) convertDTO2Bean(entity, RawAnomalyResultBean.class);
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
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;
  }

  public int update(RawAnomalyResultDTO entity) {
    RawAnomalyResultBean bean =
        (RawAnomalyResultBean) convertDTO2Bean(entity, RawAnomalyResultBean.class);
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
    return genericPojoDao.update(bean);
  }

  public RawAnomalyResultDTO findById(Long id) {
    RawAnomalyResultBean rawAnomalyResultBean = genericPojoDao.get(id, RawAnomalyResultBean.class);
    if (rawAnomalyResultBean != null) {
      RawAnomalyResultDTO rawAnomalyResultDTO;
      rawAnomalyResultDTO = createRawAnomalyDTOFromBean(rawAnomalyResultBean);
      return rawAnomalyResultDTO;
    } else {
      return null;
    }
  }

  private RawAnomalyResultDTO createRawAnomalyDTOFromBean(RawAnomalyResultBean rawAnomalyResultBean) {
    RawAnomalyResultDTO rawAnomalyResultDTO;
    rawAnomalyResultDTO = MODEL_MAPPER.map(rawAnomalyResultBean, RawAnomalyResultDTO.class);
    if (rawAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(rawAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      if (anomalyFunctionBean == null) {
        LOG.error("this anomaly function bean should not be null");
      }
      AnomalyFunctionDTO anomalyFunctionDTO = MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      rawAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (rawAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean =
          genericPojoDao.get(rawAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO = MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      rawAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    return rawAnomalyResultDTO;
  }
}
