package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import java.util.List;


public interface DatasetConfigManager extends AbstractManager<DatasetConfigDTO> {

  DatasetConfigDTO findByDataset(String dataset);
  List<DatasetConfigDTO> findActive();
  List<DatasetConfigDTO> findActiveRequiresCompletenessCheck();

}
