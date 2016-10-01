package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

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

}
