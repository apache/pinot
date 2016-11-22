package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import java.util.List;

public interface OverrideConfigManager extends AbstractManager<OverrideConfigDTO> {

  List<OverrideConfigDTO> findAllConflictByTargetType(String typeName, long windowStart,
      long windowEnd);

  List<OverrideConfigDTO> findAllConflict(long startTimeMillis, long endTimeMillis);
}
