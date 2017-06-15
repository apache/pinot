package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class AlertConfigManagerImpl extends AbstractManagerImpl<AlertConfigDTO>
    implements AlertConfigManager {
  private static final String FIND_BY_NAME_LIKE = " WHERE name like :name";

  public AlertConfigManagerImpl() {
    super(AlertConfigDTO.class, AlertConfigBean.class);
  }

  @Override
  public List<AlertConfigDTO> findByActive(boolean active) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("active", active);
    return super.findByParams(filters);
  }

  @Override
  public List<AlertConfigDTO> findWhereNameLike(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_LIKE, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result;
  }

  @Override
  public List<AlertConfigDTO> findWhereApplicationLike(String name) {
    // TODO : implement this after adding app-name in the entity
    return null;
  }
}
