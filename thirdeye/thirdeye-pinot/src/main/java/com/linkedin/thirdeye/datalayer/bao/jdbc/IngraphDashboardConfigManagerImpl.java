package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.IngraphDashboardConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.IngraphDashboardConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class IngraphDashboardConfigManagerImpl extends AbstractManagerImpl<IngraphDashboardConfigDTO>
implements IngraphDashboardConfigManager {

  public IngraphDashboardConfigManagerImpl() {
    super(IngraphDashboardConfigDTO.class, IngraphDashboardConfigBean.class);
  }

  @Override
  public IngraphDashboardConfigDTO findByName(String name) {
    Predicate predicate = Predicate.EQ("name", name);
    List<IngraphDashboardConfigBean> list = genericPojoDao.get(predicate, IngraphDashboardConfigBean.class);
    IngraphDashboardConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), IngraphDashboardConfigDTO.class);
    }
    return result;
  }


}
