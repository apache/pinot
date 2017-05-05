package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;

@Singleton
public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO>
    implements RawAnomalyResultManager {

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

  public List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    Predicate startTimePredicate;
    startTimePredicate =
        Predicate.AND(Predicate.GE("startTime", startTime), Predicate.LE("startTime", endTime));
    Predicate endTimeTimePredicate;
    endTimeTimePredicate =
        Predicate.AND(Predicate.GE("endTime", startTime), Predicate.LE("endTime", endTime));;
    Predicate functionIdPredicate = Predicate.EQ("functionId", functionId);
    Predicate finalPredicate =
        Predicate.AND(functionIdPredicate, Predicate.OR(endTimeTimePredicate, startTimePredicate));
    return findByPredicate(finalPredicate);
  }

  public List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId) {
    //    "select r from RawAnomalyResultDTO r where r.function.id = :functionId and r.merged=false "
    //        + "and r.dataMissing=:dataMissing";

    Predicate predicate = Predicate.AND(//
        Predicate.EQ("functionId", functionId), //
        Predicate.EQ("merged", false), //
        Predicate.EQ("dataMissing", false) //
    );
    return findByPredicate(predicate);
  }

  public List<RawAnomalyResultDTO> findByFunctionId(Long functionId) {
    Predicate predicate = Predicate.EQ("functionId", functionId);
    return findByPredicate(predicate);
  }
}
