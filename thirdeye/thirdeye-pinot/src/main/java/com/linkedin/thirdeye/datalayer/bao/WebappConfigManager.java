package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;


public interface WebappConfigManager extends AbstractManager<WebappConfigDTO> {

  List<WebappConfigDTO> findByCollection(String collection);

  List<WebappConfigDTO> findByType(WebappConfigType type);

  List<WebappConfigDTO> findByCollectionAndType(String collection, WebappConfigType type);

}
