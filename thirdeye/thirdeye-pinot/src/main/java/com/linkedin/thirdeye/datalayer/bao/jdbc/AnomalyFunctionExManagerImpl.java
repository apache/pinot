package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionExManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionExBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class AnomalyFunctionExManagerImpl extends AbstractManagerImpl<AnomalyFunctionExDTO>
    implements AnomalyFunctionExManager {

  static final Class<AnomalyFunctionExBean> BEAN_CLASS = AnomalyFunctionExBean.class;
  static final Class<AnomalyFunctionExDTO> DTO_CLASS = AnomalyFunctionExDTO.class;

  public AnomalyFunctionExManagerImpl() {
    super(DTO_CLASS, BEAN_CLASS);
  }

  @Override
  public List<AnomalyFunctionExDTO> findByActive(boolean active) {
    return queryAndMap(Predicate.EQ("active", active));
  }

  @Override
  public List<AnomalyFunctionExDTO> findByName(String name) {
    return queryAndMap(Predicate.EQ("name", name));
  }

  @Override
  public List<AnomalyFunctionExDTO> findByClassName(String className) {
    return queryAndMap(Predicate.EQ("className", className));
  }

  private List<AnomalyFunctionExDTO> queryAndMap(Predicate predicate) {
    List<AnomalyFunctionExBean> list = genericPojoDao.get(predicate, BEAN_CLASS);
    List<AnomalyFunctionExDTO> result = new ArrayList<>();
    for (AnomalyFunctionExBean abstractBean : list) {
      AnomalyFunctionExDTO dto = MODEL_MAPPER.map(abstractBean, DTO_CLASS);
      result.add(dto);
    }
    return result;
  }

}
