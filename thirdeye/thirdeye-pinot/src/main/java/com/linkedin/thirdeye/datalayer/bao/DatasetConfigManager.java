package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;


public interface DatasetConfigManager extends AbstractManager<DatasetConfigDTO> {

  DatasetConfigDTO findByDataset(String dataset);

}
