package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.util.Predicate;

public class DashboardConfigManagerImpl extends AbstractManagerImpl<DashboardConfigDTO>
    implements DashboardConfigManager {

  private static final String FIND_BY_NAME_LIKE = " WHERE name like :name";

  public DashboardConfigManagerImpl() {
    super(DashboardConfigDTO.class, DashboardConfigBean.class);
  }

  @Override
  public DashboardConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);
    List<DashboardConfigBean> list = genericPojoDao.get(predicate, DashboardConfigBean.class);
    DashboardConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), DashboardConfigDTO.class);
    }
    return result;
  }

  @Override
  public List<DashboardConfigDTO> findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<DashboardConfigBean> list = genericPojoDao.get(predicate, DashboardConfigBean.class);
    List<DashboardConfigDTO> result = new ArrayList<>();
    for (DashboardConfigBean abstractBean : list) {
      DashboardConfigDTO dto = MODEL_MAPPER.map(abstractBean, DashboardConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<DashboardConfigDTO> findActiveByDataset(String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate activePredicate = Predicate.EQ("active", true);
    List<DashboardConfigBean> list = genericPojoDao.get(Predicate.AND(datasetPredicate, activePredicate),
        DashboardConfigBean.class);
    List<DashboardConfigDTO> result = new ArrayList<>();
    for (DashboardConfigBean abstractBean : list) {
      DashboardConfigDTO dto = MODEL_MAPPER.map(abstractBean, DashboardConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  public List<DashboardConfigDTO> findWhereNameLike(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    List<DashboardConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, DashboardConfigBean.class);
    List<DashboardConfigDTO> result = new ArrayList<>();
    for (DashboardConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, DashboardConfigDTO.class));
    }
    return result;
  }

}
