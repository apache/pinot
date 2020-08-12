package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import org.apache.pinot.thirdeye.datalayer.bao.OnlineDetectionDataManager;
import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.OnlineDetectionDataBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

import java.util.List;

public class OnlineDetectionDataManagerImpl extends AbstractManagerImpl<OnlineDetectionDataDTO>
    implements OnlineDetectionDataManager {

  public OnlineDetectionDataManagerImpl() {
    super(OnlineDetectionDataDTO.class, OnlineDetectionDataBean.class);
  }

  @Override
  public List<OnlineDetectionDataDTO> findByDatasetAndMetric(String dataset, String metric) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate metricPredicate = Predicate.EQ("metric", metric);
    Predicate predicate = Predicate.AND(datasetPredicate, metricPredicate);

    return findByPredicate(predicate);
  }
}
