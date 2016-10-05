package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;


public interface DatasetConfigManager extends AbstractManager<DatasetConfigDTO> {

  DatasetConfigDTO findByDataset(String dataset);
  List<DatasetConfigDTO> findActive();

}
