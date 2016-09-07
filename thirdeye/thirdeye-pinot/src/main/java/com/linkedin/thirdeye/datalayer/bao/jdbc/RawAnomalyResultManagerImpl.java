package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.entity.RawAnomalyResult;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO>
    implements RawAnomalyResultManager {

  public RawAnomalyResultManagerImpl(Class<RawAnomalyResultDTO> entityClass) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    //rawAnomalyDao, feedbackDao, functionDao

    Predicate predicate = Predicate.AND( //
        Predicate.GT("startTime", startTime), //
        Predicate.LT("endTime", endTime), //
        Predicate.EQ("functionId", functionId) //
    );

    List<RawAnomalyResult> rawAnomalyResults = rawAnomalyResultDAO.findByParams(predicate);
    List<RawAnomalyResultDTO> rawAnomalyResultDTOList = new ArrayList<>();
    for (RawAnomalyResult result : rawAnomalyResults) {
      try {
        RawAnomalyResultDTO dto = new RawAnomalyResultDTO();
        RawAnomalyResultBean rawAnomalyResultBean =
            OBJECT_MAPPER.readValue(result.getJsonVal(), RawAnomalyResultBean.class);
        MODEL_MAPPER.map(rawAnomalyResultBean, dto);
        Long feedbackId = result.getAnomalyFeedbackId();
        if (feedbackId != null) {
          AnomalyFeedbackDTO anomalyFeedbackDTO = fetchAnomalyFeedback(feedbackId);
          dto.setFeedback(anomalyFeedbackDTO);
        }
        Long anomalyFunctionId = result.getAnomalyFunctionId();
        if (anomalyFunctionId != null) {
          AnomalyFunctionDTO anomalyFunctionDTO = fetchAnomalyFunction(anomalyFunctionId);
          dto.setFunction(anomalyFunctionDTO);
        }
        rawAnomalyResultDTOList.add(dto);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return rawAnomalyResultDTOList;
  }



  @Override
  public List<RawAnomalyResultDTO> findAllByTimeFunctionIdAndDimensions(long startTime,
      long endTime, long functionId, String dimensions) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<GroupByRow<GroupByKey, Long>> getCountByFunction(long startTime, long endTime) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<GroupByRow<GroupByKey, Long>> getCountByFunctionDimensions(long startTime,
      long endTime) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RawAnomalyResultDTO> findUnmergedByCollectionMetricAndDimensions(String collection,
      String metric, String dimensions) {
    // TODO Auto-generated method stub
    return null;
  }

}
