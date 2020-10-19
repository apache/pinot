package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Inject;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.bao.OnlineDetectionDataManager;
import org.apache.pinot.thirdeye.datalayer.dao.GenericPojoDao;
import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.OnlineDetectionDataBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;

public class OnlineDetectionDataManagerImpl extends AbstractManagerImpl<OnlineDetectionDataDTO>
    implements OnlineDetectionDataManager {

  @Inject
  public OnlineDetectionDataManagerImpl(GenericPojoDao genericPojoDao) {
    super(OnlineDetectionDataDTO.class, OnlineDetectionDataBean.class, genericPojoDao);
  }

  @Override
  public List<OnlineDetectionDataDTO> findByDatasetAndMetric(String dataset, String metric) {
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    Predicate metricPredicate = Predicate.EQ("metric", metric);
    Predicate predicate = Predicate.AND(datasetPredicate, metricPredicate);

    return findByPredicate(predicate);
  }
}
