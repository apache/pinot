package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;

import java.util.List;

public interface OnboardDatasetMetricManager extends AbstractManager<OnboardDatasetMetricDTO> {

  List<OnboardDatasetMetricDTO> findByDataSource(String dataSource);
  List<OnboardDatasetMetricDTO> findByDataSourceAndOnboarded(String dataSource, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByDataset(String datasetName);
  List<OnboardDatasetMetricDTO> findByDatasetAndOnboarded(String datasetName, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByMetric(String metricName);
  List<OnboardDatasetMetricDTO> findByMetricAndOnboarded(String metricName, boolean onboarded);
  List<OnboardDatasetMetricDTO> findByDatasetAndDatasource(String datasetName, String dataSource);
  List<OnboardDatasetMetricDTO> findByDatasetAndDatasourceAndOnboarded(String datasetName, String dataSource, boolean onboarded);


}
