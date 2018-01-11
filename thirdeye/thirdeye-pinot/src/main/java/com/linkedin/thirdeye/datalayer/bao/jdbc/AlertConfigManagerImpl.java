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
  private static final String FIND_BY_NAME_EQUALS = " WHERE name = :name";
  private static final String FIND_BY_APPLICATION_LIKE = " WHERE application like :application";

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
  public AlertConfigDTO findWhereNameEquals(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_EQUALS, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result.isEmpty()? null : result.get(0);
  }

  @Override
  public List<AlertConfigDTO> findWhereApplicationLike(String application) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("application", application);
    List<AlertConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_APPLICATION_LIKE, parameterMap, AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
    }
    return result;
  }

  @Override
  public List<AlertConfigDTO> findByFunctionId(Long functionId) {
    List<AlertConfigBean> list = genericPojoDao.getAll(AlertConfigBean.class);
    List<AlertConfigDTO> result = new ArrayList<>();
    for (AlertConfigBean bean : list) {
      if (bean.getEmailConfig() != null && bean.getEmailConfig().getFunctionIds().contains(functionId)) {
        result.add(MODEL_MAPPER.map(bean, AlertConfigDTO.class));
      }
    }
    return result;
  }

}
