package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DataCompletenessConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class DataCompletenessConfigManagerImpl extends AbstractManagerImpl<DataCompletenessConfigDTO> implements DataCompletenessConfigManager {


  public DataCompletenessConfigManagerImpl() {
    super(DataCompletenessConfigDTO.class, DataCompletenessConfigBean.class);
  }

  @Override
  public List<DataCompletenessConfigDTO> findAllByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<DataCompletenessConfigBean> list = genericPojoDao.get(predicate, DataCompletenessConfigBean.class);
    return convertListOfBeanToDTO(list);
  }

  @Override
  public List<DataCompletenessConfigDTO> findAllInTimeRange(long startTime, long endTime) {
    Predicate timePredicate = Predicate.AND(Predicate.GT("dateToCheckInMS", startTime), Predicate.LT("dateToCheckInMS", endTime));
    List<DataCompletenessConfigBean> list =
        genericPojoDao.get(timePredicate, DataCompletenessConfigBean.class);
    return convertListOfBeanToDTO(list);
  }

  @Override
  public List<DataCompletenessConfigDTO> findAllByDatasetAndInTimeRange(String dataset, long startTime, long endTime) {
    Predicate timePredicate = Predicate.AND(Predicate.GT("dateToCheckInMS", startTime), Predicate.LT("dateToCheckInMS", endTime));
    Predicate datasetPredicate = Predicate.EQ("dataset", dataset);
    List<DataCompletenessConfigBean> list =
        genericPojoDao.get(Predicate.AND(datasetPredicate, timePredicate), DataCompletenessConfigBean.class);
    return convertListOfBeanToDTO(list);
  }

  @Override
  public List<DataCompletenessConfigDTO> findAllByTimeOlderThan(long time) {
    Predicate predicate = Predicate.LT("dateToCheckInMS", time);
    List<DataCompletenessConfigBean> list = genericPojoDao.get(predicate, DataCompletenessConfigBean.class);
    return convertListOfBeanToDTO(list);
  }

  @Override
  public List<DataCompletenessConfigDTO> findAllByTimeOlderThanAndStatus(long time, boolean dataComplete) {
    Predicate datePredicate = Predicate.LT("dateToCheckInMS", time);
    Predicate dataCompletePredicate = Predicate.EQ("dataComplete", dataComplete);
    List<DataCompletenessConfigBean> list =
        genericPojoDao.get(Predicate.AND(datePredicate, dataCompletePredicate), DataCompletenessConfigBean.class);
    return convertListOfBeanToDTO(list);
  }

  private List<DataCompletenessConfigDTO> convertListOfBeanToDTO(List<DataCompletenessConfigBean> list) {
    List<DataCompletenessConfigDTO> results = new ArrayList<>();
    for (DataCompletenessConfigBean abstractBean : list) {
      DataCompletenessConfigDTO dto = MODEL_MAPPER.map(abstractBean, DataCompletenessConfigDTO.class);
      results.add(dto);
    }
    return results;
  }


}
