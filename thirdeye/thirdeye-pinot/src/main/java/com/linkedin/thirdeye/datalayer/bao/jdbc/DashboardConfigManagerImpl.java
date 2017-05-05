package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class DashboardConfigManagerImpl extends AbstractManagerImpl<DashboardConfigDTO>
    implements DashboardConfigManager {

  private static final String FIND_BY_NAME_LIKE = " WHERE active = :active and name like :name";

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
    return findByPredicate(predicate);
  }

  @Override
  public List<DashboardConfigDTO> findActiveByDataset(String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate activePredicate = Predicate.EQ("active", true);
    return findByPredicate(Predicate.AND(datasetPredicate, activePredicate));
  }

  public List<DashboardConfigDTO> findWhereNameLikeAndActive(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    parameterMap.put("active", true);
    List<DashboardConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, DashboardConfigBean.class);
    return convertBeanListToDTOList(list);
  }
}
