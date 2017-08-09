package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

@Singleton
public class MetricConfigManagerImpl extends AbstractManagerImpl<MetricConfigDTO>
    implements MetricConfigManager {

  private static final String FIND_BY_NAME_OR_ALIAS_LIKE = " WHERE active = :active and (alias like :name or name like :name)";

  private static final String FIND_BY_ALIAS_LIKE = " WHERE active = :active";
  private static final String FIND_BY_ALIAS_LIKE_PART = " AND alias LIKE :alias__%d";

  public MetricConfigManagerImpl() {
    super(MetricConfigDTO.class, MetricConfigBean.class);
  }

  @Override
  public List<MetricConfigDTO> findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    return findByPredicate(predicate);
  }

  @Override
  public List<MetricConfigDTO> findActiveByDataset(String dataset) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate activePredicate = Predicate.EQ("active", true);
    Predicate predicate = Predicate.AND(datasetPredicate, activePredicate);
    return findByPredicate(predicate);
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
    return findByPredicate(metricNamePredicate);
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

  @Override
  public List<MetricConfigDTO> findWhereNameOrAliasLikeAndActive(String name) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("name", name);
    parameterMap.put("active", true);
    List<MetricConfigBean> list =
        genericPojoDao.executeParameterizedSQL(FIND_BY_NAME_OR_ALIAS_LIKE, parameterMap, MetricConfigBean.class);
    List<MetricConfigDTO> result = new ArrayList<>();
    for (MetricConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, MetricConfigDTO.class));
    }
    return result;
  }

  @Override
  public List<MetricConfigDTO> findWhereAliasLikeAndActive(Set<String> aliasParts) {
    StringBuilder query = new StringBuilder();
    query.append(FIND_BY_ALIAS_LIKE);

    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("active", true);
    int i = 0;
    for (String n : aliasParts) {
      query.append(String.format(FIND_BY_ALIAS_LIKE_PART, i));
      parameterMap.put(String.format("alias__%d", i), "%" + n + "%"); // using field name decomposition
      i++;
    }

    List<MetricConfigBean> list =
        genericPojoDao.executeParameterizedSQL(query.toString(), parameterMap, MetricConfigBean.class);
    List<MetricConfigDTO> result = new ArrayList<>();
    for (MetricConfigBean bean : list) {
      result.add(MODEL_MAPPER.map(bean, MetricConfigDTO.class));
    }
    return result;
  }
}
