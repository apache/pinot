package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;

public interface DataCompletenessConfigManager extends AbstractManager<DataCompletenessConfigDTO>{

  List<DataCompletenessConfigDTO> findAllByDataset(String dataset);
  List<DataCompletenessConfigDTO> findAllInTimeRange(long startTime, long endTime);
  List<DataCompletenessConfigDTO> findAllByDatasetAndInTimeRange(String dataset, long startTime, long endTime);
  List<DataCompletenessConfigDTO> findAllByTimeOlderThan(long time);
  List<DataCompletenessConfigDTO> findAllByTimeOlderThanAndStatus(long time, boolean dataComplete);

}
