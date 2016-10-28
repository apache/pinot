package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.IngraphMetricConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class IngraphMetricConfigManagerImpl extends AbstractManagerImpl<IngraphMetricConfigDTO>
implements IngraphMetricConfigManager {

  public IngraphMetricConfigManagerImpl() {
    super(IngraphMetricConfigDTO.class, IngraphMetricConfigBean.class);
  }

  @Override
  public List<IngraphMetricConfigDTO> findByDataset(String dataset) {
    Predicate predicate = Predicate.EQ("dataset", dataset);
    List<IngraphMetricConfigBean> list = genericPojoDao.get(predicate, IngraphMetricConfigBean.class);
    List<IngraphMetricConfigDTO> result = new ArrayList<>();
    for (IngraphMetricConfigBean abstractBean : list) {
      IngraphMetricConfigDTO dto = MODEL_MAPPER.map(abstractBean, IngraphMetricConfigDTO.class);
      result.add(dto);
    }
    return result;
  }
}
