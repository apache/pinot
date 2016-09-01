package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.entity.IngraphMetricConfig;
import com.linkedin.thirdeye.db.entity.WebappConfig;
import java.util.List;

public class IngraphMetricConfigManager extends AbstractManager<IngraphMetricConfigDTO, IngraphMetricConfig> {

  public List<WebappConfig> findByCollection(String collection) {
    return null;
  }

  public List<WebappConfig> findByType(WebappConfigType type) {
    return null;
  }

  public List<WebappConfig> findByCollectionAndType(String collection, WebappConfigType type) {
    return null;
  }
}
