package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import com.linkedin.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import com.linkedin.thirdeye.datalayer.pojo.OnboardDatasetMetricBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

import java.util.List;

@Singleton
public class OnboardDatasetMetricManagerImpl extends AbstractManagerImpl<OnboardDatasetMetricDTO>
    implements OnboardDatasetMetricManager {

  public OnboardDatasetMetricManagerImpl() {
    super(OnboardDatasetMetricDTO.class, OnboardDatasetMetricBean.class);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataSource(String dataSource) {
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    return findByPredicate(dataSourcePredicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataSourceAndOnboarded(String dataSource,
      boolean onboarded) {
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(dataSourcePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDataset(String datasetName) {
    Predicate predicate = Predicate.EQ("datasetName", datasetName);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndOnboarded(String datasetName,
      boolean onboarded) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(datasetNamePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByMetric(String metricName) {
    Predicate predicate = Predicate.EQ("metricName", metricName);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByMetricAndOnboarded(String metricName, boolean onboarded) {
    Predicate metricNamePredicate = Predicate.EQ("metricName", metricName);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(metricNamePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndDatasource(String datasetName,
      String dataSource) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate predicate = Predicate.AND(datasetNamePredicate, dataSourcePredicate);
    return findByPredicate(predicate);
  }

  @Override
  public List<OnboardDatasetMetricDTO> findByDatasetAndDatasourceAndOnboarded(String datasetName,
      String dataSource, boolean onboarded) {
    Predicate datasetNamePredicate = Predicate.EQ("datasetName", datasetName);
    Predicate dataSourcePredicate = Predicate.EQ("dataSource", dataSource);
    Predicate onboardedPredicate = Predicate.EQ("onboarded", onboarded);
    Predicate predicate = Predicate.AND(datasetNamePredicate, dataSourcePredicate, onboardedPredicate);
    return findByPredicate(predicate);
  }


}
