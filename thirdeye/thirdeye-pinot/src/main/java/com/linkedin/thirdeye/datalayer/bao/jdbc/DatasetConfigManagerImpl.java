package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

public class DatasetConfigManagerImpl extends AbstractManagerImpl<DatasetConfigDTO>
    implements DatasetConfigManager {

  public DatasetConfigManagerImpl() {
    super(DatasetConfigDTO.class, DatasetConfigBean.class);
  }


  @Override
  public DatasetConfigDTO findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<DatasetConfigBean> list = genericPojoDao.get(predicate, DatasetConfigBean.class);
    DatasetConfigDTO result = null;
    if (CollectionUtils.isNotEmpty(list)) {
      result = MODEL_MAPPER.map(list.get(0), DatasetConfigDTO.class);
    }
    return result;
  }

  @Override
  public List<DatasetConfigDTO> findActive() {
    Predicate activePredicate = Predicate.EQ("active", true);
    List<DatasetConfigBean> list = genericPojoDao.get(activePredicate, DatasetConfigBean.class);
    List<DatasetConfigDTO> results = new ArrayList<>();
    for (DatasetConfigBean abstractBean : list) {
      DatasetConfigDTO result = MODEL_MAPPER.map(abstractBean, DatasetConfigDTO.class);
      results.add(result);
    }
    return results;
  }

  @Override
  public List<DatasetConfigDTO> findActiveRequiresCompletenessCheck() {
    Predicate activePredicate = Predicate.EQ("active", true);
    Predicate completenessPredicate = Predicate.EQ("requiresCompletenessCheck", true);
    List<DatasetConfigBean> list = genericPojoDao.get(Predicate.AND(activePredicate, completenessPredicate), DatasetConfigBean.class);
    List<DatasetConfigDTO> results = new ArrayList<>();
    for (DatasetConfigBean abstractBean : list) {
      DatasetConfigDTO result = MODEL_MAPPER.map(abstractBean, DatasetConfigDTO.class);
      results.add(result);
    }
    return results;
  }
}
