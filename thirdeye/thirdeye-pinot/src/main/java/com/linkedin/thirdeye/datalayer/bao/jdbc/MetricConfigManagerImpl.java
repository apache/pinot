package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class MetricConfigManagerImpl extends AbstractManagerImpl<MetricConfigDTO>
    implements MetricConfigManager {

  public MetricConfigManagerImpl() {
    super(MetricConfigDTO.class, MetricConfigBean.class);
  }

  @Override
  public List<MetricConfigDTO> findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<MetricConfigBean> list = genericPojoDao.get(predicate, MetricConfigBean.class);
    List<MetricConfigDTO> result = new ArrayList<>();
    for (MetricConfigBean abstractBean : list) {
      MetricConfigDTO dto = MODEL_MAPPER.map(abstractBean, MetricConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<MetricConfigDTO> findActiveByDataset(String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate activePredicate = Predicate.EQ("active", true);
    List<MetricConfigBean> list = genericPojoDao.get(Predicate.AND(datasetPredicate, activePredicate),
        MetricConfigBean.class);
    List<MetricConfigDTO> result = new ArrayList<>();
    for (MetricConfigBean abstractBean : list) {
      MetricConfigDTO dto = MODEL_MAPPER.map(abstractBean, MetricConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public MetricConfigDTO findByMetricAndDataset(String metricName, String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate metricNamePredicate = Predicate.EQ("name", metricName);
    List<MetricConfigBean> list = genericPojoDao.get(Predicate.AND(datasetPredicate, metricNamePredicate),
        MetricConfigBean.class);
    MetricConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), MetricConfigDTO.class);
    }
    return result;
  }

  public List<MetricConfigDTO> findByMetricName(String metricName) {
    Predicate metricNamePredicate = Predicate.EQ("name", metricName);
    List<MetricConfigBean> list = genericPojoDao.get(metricNamePredicate, MetricConfigBean.class);
    List<MetricConfigDTO> result = new ArrayList<>();
    for (MetricConfigBean abstractBean : list) {
      MetricConfigDTO dto = MODEL_MAPPER.map(abstractBean, MetricConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public MetricConfigDTO findByAliasAndDataset(String alias, String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate aliasPredicate = Predicate.EQ("alias", alias);
    List<MetricConfigBean> list = genericPojoDao.get(Predicate.AND(datasetPredicate, aliasPredicate),
        MetricConfigBean.class);
    MetricConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), MetricConfigDTO.class);
    }
    return result;
  }


}
