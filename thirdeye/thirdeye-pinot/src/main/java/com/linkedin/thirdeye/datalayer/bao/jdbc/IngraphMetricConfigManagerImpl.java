package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.IngraphMetricConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class IngraphMetricConfigManagerImpl extends AbstractManagerImpl<IngraphMetricConfigDTO>
implements IngraphMetricConfigManager {

  public IngraphMetricConfigManagerImpl() {
    super(IngraphMetricConfigDTO.class, IngraphMetricConfigBean.class);
  }

  @Override
  public List<IngraphMetricConfigDTO> findByDashboard(String dashboardName) {
    Predicate predicate = Predicate.EQ("dashboardName", dashboardName);
    List<IngraphMetricConfigBean> list = genericPojoDao.get(predicate, IngraphMetricConfigBean.class);
    List<IngraphMetricConfigDTO> result = new ArrayList<>();
    for (IngraphMetricConfigBean abstractBean : list) {
      IngraphMetricConfigDTO dto = MODEL_MAPPER.map(abstractBean, IngraphMetricConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public IngraphMetricConfigDTO findByDashboardAndMetricName(String dashboardName, String metricName) {
    Predicate dashboardPredicate = Predicate.EQ("dashboardName", dashboardName);
    Predicate metricPredicate = Predicate.EQ("metricName", metricName);
    List<IngraphMetricConfigBean> list = genericPojoDao.get(Predicate.AND(dashboardPredicate, metricPredicate),
        IngraphMetricConfigBean.class);
    IngraphMetricConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), IngraphMetricConfigDTO.class);
    }
    return result;
  }

  @Override
  public IngraphMetricConfigDTO findByRrdName(String rrdName) {
    Predicate dashboardPredicate = Predicate.EQ("rrdName", rrdName);
    List<IngraphMetricConfigBean> list = genericPojoDao.get(dashboardPredicate, IngraphMetricConfigBean.class);
    IngraphMetricConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), IngraphMetricConfigDTO.class);
    }
    return result;
  }
}
