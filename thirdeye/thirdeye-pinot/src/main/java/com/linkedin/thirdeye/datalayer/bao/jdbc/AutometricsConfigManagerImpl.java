package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.AutometricsConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AutometricsConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AutometricsConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class AutometricsConfigManagerImpl extends AbstractManagerImpl<AutometricsConfigDTO>
implements AutometricsConfigManager {

  public AutometricsConfigManagerImpl() {
    super(AutometricsConfigDTO.class, AutometricsConfigBean.class);
  }

  @Override
  public List<AutometricsConfigDTO> findByDashboard(String dashboard) {
    Predicate predicate = Predicate.EQ("dashboard", dashboard);
    return findByPredicate(predicate);
  }

  @Override
  public AutometricsConfigDTO findByDashboardAndMetric(String dashboard, String metric) {
    Predicate dashboardPredicate = Predicate.EQ("dashboard", dashboard);
    Predicate metricPredicate = Predicate.EQ("metric", metric);
    List<AutometricsConfigBean> list = genericPojoDao.get(Predicate.AND(dashboardPredicate, metricPredicate),
        AutometricsConfigBean.class);
    AutometricsConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), AutometricsConfigDTO.class);
    }
    return result;
  }

  @Override
  public AutometricsConfigDTO findByRrd(String rrd) {
    Predicate rrdPredicate = Predicate.EQ("rrd", rrd);
    List<AutometricsConfigBean> list = genericPojoDao.get(rrdPredicate, AutometricsConfigBean.class);
    AutometricsConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), AutometricsConfigDTO.class);
    }
    return result;
  }
}
