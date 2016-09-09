package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.modelmapper.ModelMapper;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dao.AnomalyFeedbackDAO;
import com.linkedin.thirdeye.datalayer.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.datalayer.dao.RawAnomalyResultDAO;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFeedback;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();
  protected static RawAnomalyResultDAO rawAnomalyResultDAO =
      DaoProviderUtil.getInstance(RawAnomalyResultDAO.class);
  protected static AnomalyFeedbackDAO anomalyFeedbackDAO =
      DaoProviderUtil.getInstance(AnomalyFeedbackDAO.class);
  protected static AnomalyFunctionDAO anomalyFunctionDAO =
      DaoProviderUtil.getInstance(AnomalyFunctionDAO.class);

  @Override
  public Long save(E entity) {
    return null;
  }

  @Override
  public void update(E entity) {

  }

  @Override
  public E findById(Long id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void delete(E entity) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteById(Long id) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<E> findAll() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<E> findByParams(Map<String, Object> filters) {
    // TODO Auto-generated method stub
    return null;
  }

  protected AnomalyFunctionDTO fetchAnomalyFunction(Long anomalyFunctionId)
      throws IOException, JsonParseException, JsonMappingException {
    AnomalyFunction anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
    AnomalyFunctionBean anomalyFunctionBean =
        OBJECT_MAPPER.readValue(anomalyFunction.getJsonVal(), AnomalyFunctionBean.class);
    AnomalyFunctionDTO anomalyFunctionDTO = new AnomalyFunctionDTO();
    MODEL_MAPPER.map(anomalyFunctionDTO, anomalyFunctionBean);
    return anomalyFunctionDTO;
  }

  protected AnomalyFeedbackDTO fetchAnomalyFeedback(Long feedbackId)
      throws IOException, JsonParseException, JsonMappingException {
    AnomalyFeedback anomalyFeedback = anomalyFeedbackDAO.findById(feedbackId);
    AnomalyFeedbackBean anomalyFeedbackBean =
        OBJECT_MAPPER.readValue(anomalyFeedback.getJsonVal(), AnomalyFeedbackBean.class);
    AnomalyFeedbackDTO anomalyFeedbackDTO = new AnomalyFeedbackDTO();
    MODEL_MAPPER.map(anomalyFeedbackDTO, anomalyFeedbackBean);
    return anomalyFeedbackDTO;
  }
}
