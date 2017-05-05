package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

@Singleton
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
    return findByPredicate(activePredicate);
  }

  @Override
  public List<DatasetConfigDTO> findActiveRequiresCompletenessCheck() {
    Predicate activePredicate = Predicate.EQ("active", true);
    Predicate completenessPredicate = Predicate.EQ("requiresCompletenessCheck", true);
    Predicate predicate = Predicate.AND(activePredicate, completenessPredicate);
    return findByPredicate(predicate);
  }
}
