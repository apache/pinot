package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
    implements AnomalyFunctionManager {
  private static final String FIND_BY_NAME_LIKE = " WHERE functionName like :functionName";
  private static final String FIND_BY_NAME_EQUALS = " WHERE functionName = :functionName";

  public AnomalyFunctionManagerImpl() {
    super(AnomalyFunctionDTO.class, AnomalyFunctionBean.class);
  }

  @Override
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean abstractBean : list) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(abstractBean, AnomalyFunctionDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<String> findDistinctTopicMetricsByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionDTO> dtoList = findByPredicate(predicate);
    Set<String> metrics = new HashSet<>();
    for (AnomalyFunctionDTO dto : dtoList) {
      metrics.add(dto.getTopicMetric());
    }
    return new ArrayList<>(metrics);
  }

  @Override
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    Predicate predicate = Predicate.EQ("active", true);
    return findByPredicate(predicate);
  }

  @Override
  public List<AnomalyFunctionDTO> findWhereNameLike(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionName", name);
    List<AnomalyFunctionBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AnomalyFunctionDTO.class));
    }
    return result;
  }

  @Override
  public AnomalyFunctionDTO findWhereNameEquals(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("functionName", name);
    List<AnomalyFunctionBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_EQUALS, parameterMap, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AnomalyFunctionDTO.class));
    }
    return result.isEmpty()? null : result.get(0);
  }
}
